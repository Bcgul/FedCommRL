// nfqdelay: userspace replacement for tc-netem delay/jitter/loss, using NFQUEUE.
// One process serves up to 2 queues (e.g. uplink OUTPUT and downlink INPUT of one namespace).
// Params per queue are read from a file "<qnum> <delay_ms> <jitter_ms> <loss_pct> [burst]" lines; reloaded on SIGHUP.
// burst > 1 selects Gilbert-Elliott loss: a two-state chain whose bad state drops every packet, with mean
// bad-state length `burst` packets and stationary loss `loss_pct` (burst <= 1: independent Bernoulli loss).
// loss_pct = 100 emulates an outage.
// Release order is FIFO per queue (release = max(prev_release, now + delay + U(-J,J))), i.e. jitter never
// reorders packets (comparable to netem with a rate/pfifo child; avoids artificial reordering).
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <poll.h>
#include <errno.h>
#include <arpa/inet.h>
#include <linux/netfilter.h>
#include <libnetfilter_queue/libnetfilter_queue.h>

#define MAXQ 2
#define RING 262144

typedef struct { unsigned int id; long long rel; } item_t;
typedef struct {
  int qnum; struct nfq_q_handle *qh;
  double delay_ms, jitter_ms, loss, burst; int bad;
  item_t *ring; unsigned head, tail; long long last_rel;
  unsigned long long seen, dropped, overflow;
} q_t;

static q_t Q[MAXQ]; static int nq = 0; static const char *pfile; static volatile int reload = 1, stop = 0;
static unsigned long long rng = 88172645463325252ULL;
static double urand(void){ rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return (rng >> 11) * (1.0/9007199254740992.0); }
static long long now_ns(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec*1000000000LL + t.tv_nsec; }

static void load_params(void){
  FILE *f = fopen(pfile, "r"); if(!f) return;
  int qn; double d,j,l,b; char line[256];
  while(fgets(line, sizeof line, f)){
    b = 1; int k = sscanf(line, "%d %lf %lf %lf %lf", &qn, &d, &j, &l, &b);
    if(k < 4) continue;
    for(int i=0;i<nq;i++) if(Q[i].qnum==qn){ Q[i].delay_ms=d; Q[i].jitter_ms=j; Q[i].loss=l/100.0; Q[i].burst=b; }
  }
  fclose(f);
}

static int cb(struct nfq_q_handle *qh, struct nfgenmsg *m, struct nfq_data *nfa, void *data){
  q_t *q = (q_t*)data;
  struct nfqnl_msg_packet_hdr *ph = nfq_get_msg_packet_hdr(nfa);
  unsigned int id = ntohl(ph->packet_id);
  q->seen++;
  int drop = 0;
  if(q->loss >= 1.0) drop = 1;
  else if(q->loss > 0 && q->burst <= 1.0) drop = urand() < q->loss;
  else if(q->loss > 0){  /* Gilbert-Elliott: P(bad->good)=1/burst, P(good->bad)=loss/(burst*(1-loss)) */
    if(q->bad){ if(urand() < 1.0/q->burst) q->bad = 0; }
    else if(urand() < q->loss/(q->burst*(1.0-q->loss))) q->bad = 1;
    drop = q->bad;
  }
  if(drop){ q->dropped++; return nfq_set_verdict(qh, id, NF_DROP, 0, NULL); }
  double d = q->delay_ms + (q->jitter_ms > 0 ? (2*urand()-1)*q->jitter_ms : 0);
  if(d <= 0 && q->head == q->tail) return nfq_set_verdict(qh, id, NF_ACCEPT, 0, NULL);
  long long rel = now_ns() + (long long)(d*1e6);
  if(rel < q->last_rel) rel = q->last_rel;  // FIFO
  q->last_rel = rel;
  unsigned nt = (q->tail+1) % RING;
  if(nt == q->head){ q->overflow++; return nfq_set_verdict(qh, id, NF_ACCEPT, 0, NULL); }
  q->ring[q->tail].id = id; q->ring[q->tail].rel = rel; q->tail = nt;
  return 0;
}

static void on_hup(int s){ (void)s; reload = 1; }
static void on_term(int s){ (void)s; stop = 1; }

int main(int argc, char **argv){
  if(argc < 3){ fprintf(stderr, "usage: nfqdelay paramfile q1 [q2]\n"); return 1; }
  pfile = argv[1];
  struct nfq_handle *h = nfq_open(); if(!h){ perror("nfq_open"); return 1; }
  nfq_unbind_pf(h, AF_INET); nfq_bind_pf(h, AF_INET);
  for(int a=2; a<argc && nq<MAXQ; a++){
    q_t *q = &Q[nq]; memset(q, 0, sizeof(*q)); q->qnum = atoi(argv[a]);
    q->ring = calloc(RING, sizeof(item_t));
    q->qh = nfq_create_queue(h, q->qnum, &cb, q);
    if(!q->qh){ fprintf(stderr, "create_queue %d failed\n", q->qnum); return 1; }
    nfq_set_mode(q->qh, NFQNL_COPY_META, 0);
    nfq_set_queue_maxlen(q->qh, RING-1);
    nq++;
  }
  int fd = nfq_fd(h);
  int bufsz = 64*1024*1024; setsockopt(fd, SOL_SOCKET, SO_RCVBUFFORCE, &bufsz, sizeof(bufsz));
  nfnl_rcvbufsiz(nfq_nfnlh(h), bufsz);
  signal(SIGHUP, on_hup); signal(SIGTERM, on_term); signal(SIGINT, on_term);
  char buf[65536] __attribute__((aligned));
  rng ^= (unsigned long long)now_ns();
  while(!stop){
    if(reload){ reload = 0; load_params(); }
    long long t = now_ns(), next = -1;
    for(int i=0;i<nq;i++){
      q_t *q=&Q[i];
      while(q->head != q->tail && q->ring[q->head].rel <= t){
        nfq_set_verdict(q->qh, q->ring[q->head].id, NF_ACCEPT, 0, NULL);
        q->head = (q->head+1) % RING;
      }
      if(q->head != q->tail){ long long r = q->ring[q->head].rel; if(next<0 || r<next) next=r; }
    }
    int to = next < 0 ? 200 : (int)((next - t)/1000000LL);
    if(to < 0) to = 0;
    struct pollfd p = { .fd = fd, .events = POLLIN };
    int pr = poll(&p, 1, to);
    if(pr > 0 && (p.revents & POLLIN)){
      for(int k=0;k<256;k++){
        int rv = recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
        if(rv < 0) break;
        nfq_handle_packet(h, buf, rv);
      }
    }
  }
  for(int i=0;i<nq;i++){
    fprintf(stderr, "queue %d seen %llu dropped %llu overflow %llu\n", Q[i].qnum, Q[i].seen, Q[i].dropped, Q[i].overflow);
    nfq_destroy_queue(Q[i].qh);
  }
  nfq_close(h);
  return 0;
}
