# Phase 3 live results: per-client, per-direction selection on dynamic traces

5 clients, 2.5 MB model, CUBIC everywhere, 900 s identical trace per arm and repetition; metric = seconds per successful round over the horizon (lower is better). RL = the FedCommRL agent (dual Q-tables, alpha 0.1, gamma 0.9, epsilon 0.95^t, equal reward weights). `_hetero` = every client has its own trace.

Runs completed per cell:

| kind                |   bolr |   grpc |   http3 |   rl |
|:--------------------|-------:|-------:|--------:|-----:|
| flip                |      3 |      3 |       3 |    3 |
| intermittent_hetero |      3 |      3 |       3 |    3 |
| mobility            |      3 |      3 |       3 |    3 |
| mobility_hetero     |      3 |      3 |       3 |    3 |

## Seconds per successful round (median over repetitions)

| kind                |   http3 |   grpc |   bolr |   rl |
|:--------------------|--------:|-------:|-------:|-----:|
| flip                |     4.4 |    5.2 |    4.9 |  9   |
| intermittent_hetero |    17.3 |   21.4 |   19.6 | 20.5 |
| mobility            |    11.5 |   22.5 |   42.9 | 36   |
| mobility_hetero     |    23.1 |   33.3 |   40.9 | 50   |

## Paired with fixed HTTP/3 (same trace and repetition), pooled over traces

| arm        |   pairs |   geo_mean_ratio_vs_http3 |   min_ratio |   max_ratio | slower_in   |   wilcoxon_p |
|:-----------|--------:|--------------------------:|------------:|------------:|:------------|-------------:|
| fixed gRPC |      12 |                    1.4088 |      1.1782 |      1.95   | 12/12       |       0.0005 |
| BOLR       |      12 |                    1.6598 |      1.1064 |      4.3333 | 12/12       |       0.0005 |
| FedCommRL  |      12 |                    1.9531 |      1.1556 |      4.1053 | 12/12       |       0.0005 |

## Per run

| kind                | arm   |   rep |   ok_rounds |   failed |   s_per_round |   cpu_s_per_round |   eps_end |   switches_per_client_round |   dl_http3 |   ul_http3 |   dl_grpc |   ul_grpc |   dl_mqtt |   ul_mqtt |   dl_amqp |   ul_amqp |
|:--------------------|:------|------:|------------:|---------:|--------------:|------------------:|----------:|----------------------------:|-----------:|-----------:|----------:|----------:|----------:|----------:|----------:|----------:|
| flip                | bolr  |     0 |         186 |        0 |         4.839 |             0.435 |   nan     |                       0.081 |      0.734 |      0.717 |     0.11  |     0.123 |     0.096 |     0.084 |     0.06  |     0.076 |
| flip                | bolr  |     1 |         184 |        0 |         4.891 |             0.522 |   nan     |                       0.075 |      0.731 |      0.697 |     0.096 |     0.081 |     0.11  |     0.064 |     0.063 |     0.158 |
| flip                | bolr  |     2 |         183 |        0 |         4.918 |             0.534 |   nan     |                       0.08  |      0.735 |      0.66  |     0.011 |     0.085 |     0.122 |     0.122 |     0.133 |     0.134 |
| flip                | grpc  |     0 |         171 |        0 |         5.263 |             0.17  |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| flip                | grpc  |     1 |         172 |        0 |         5.233 |             0.206 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| flip                | grpc  |     2 |         174 |        0 |         5.172 |             0.201 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| flip                | http3 |     0 |         206 |        0 |         4.369 |             0.514 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| flip                | http3 |     1 |         205 |        0 |         4.39  |             0.663 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| flip                | http3 |     2 |         205 |        0 |         4.39  |             0.645 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| flip                | rl    |     0 |         100 |        0 |         9     |             0.222 |     0.01  |                       0.941 |      0.125 |      0.077 |     0.261 |     0.356 |     0.311 |     0.257 |     0.303 |     0.309 |
| flip                | rl    |     1 |          98 |        0 |         9.184 |             0.275 |     0.01  |                       0.952 |      0.113 |      0.085 |     0.251 |     0.309 |     0.311 |     0.253 |     0.325 |     0.354 |
| flip                | rl    |     2 |         103 |        0 |         8.738 |             0.267 |     0.01  |                       0.937 |      0.108 |      0.083 |     0.271 |     0.35  |     0.321 |     0.302 |     0.3   |     0.265 |
| intermittent_hetero | bolr  |     0 |          45 |        0 |        20     |             0.391 |   nan     |                       0.374 |      0.174 |      0.27  |     0.274 |     0.322 |     0.317 |     0.252 |     0.235 |     0.157 |
| intermittent_hetero | bolr  |     1 |          46 |        0 |        19.565 |             0.468 |   nan     |                       0.447 |      0.213 |      0.26  |     0.179 |     0.255 |     0.23  |     0.23  |     0.379 |     0.255 |
| intermittent_hetero | bolr  |     2 |          47 |        0 |        19.149 |             0.547 |   nan     |                       0.375 |      0.412 |      0.221 |     0.204 |     0.333 |     0.162 |     0.254 |     0.221 |     0.192 |
| intermittent_hetero | grpc  |     0 |          42 |        0 |        21.429 |             0.195 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| intermittent_hetero | grpc  |     1 |          43 |        0 |        20.93  |             0.218 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| intermittent_hetero | grpc  |     2 |          42 |        0 |        21.429 |             0.209 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| intermittent_hetero | http3 |     0 |          53 |        0 |        16.981 |             0.987 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| intermittent_hetero | http3 |     1 |          52 |        0 |        17.308 |             1.29  |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| intermittent_hetero | http3 |     2 |          52 |        0 |        17.308 |             1.252 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| intermittent_hetero | rl    |     0 |          44 |        0 |        20.455 |             0.3   |     0.099 |                       0.938 |      0.133 |      0.133 |     0.258 |     0.307 |     0.307 |     0.271 |     0.302 |     0.289 |
| intermittent_hetero | rl    |     1 |          45 |        0 |        20     |             0.346 |     0.094 |                       0.93  |      0.126 |      0.13  |     0.261 |     0.283 |     0.304 |     0.265 |     0.309 |     0.322 |
| intermittent_hetero | rl    |     2 |          43 |        0 |        20.93  |             0.371 |     0.105 |                       0.909 |      0.132 |      0.168 |     0.255 |     0.282 |     0.323 |     0.277 |     0.291 |     0.273 |
| mobility            | bolr  |     0 |          21 |        0 |        42.857 |             0.426 |   nan     |                       0.718 |      0.291 |      0.282 |     0.227 |     0.209 |     0.355 |     0.291 |     0.127 |     0.218 |
| mobility            | bolr  |     1 |          36 |        0 |        25     |             0.517 |   nan     |                       0.319 |      0.551 |      0.292 |     0.054 |     0.324 |     0.319 |     0.135 |     0.076 |     0.249 |
| mobility            | bolr  |     2 |          18 |        0 |        50     |             0.517 |   nan     |                       0.632 |      0.326 |      0.274 |     0.168 |     0.253 |     0.232 |     0.253 |     0.274 |     0.221 |
| mobility            | grpc  |     0 |          45 |        0 |        20     |             0.196 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| mobility            | grpc  |     1 |          32 |        0 |        28.125 |             0.25  |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| mobility            | grpc  |     2 |          40 |        0 |        22.5   |             0.232 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| mobility            | http3 |     0 |          81 |        0 |        11.111 |             0.689 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| mobility            | http3 |     1 |          60 |        0 |        15     |             0.983 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| mobility            | http3 |     2 |          78 |        0 |        11.538 |             0.843 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| mobility            | rl    |     0 |          30 |        0 |        30     |             0.283 |     0.204 |                       0.929 |      0.194 |      0.181 |     0.226 |     0.271 |     0.284 |     0.258 |     0.297 |     0.29  |
| mobility            | rl    |     1 |          25 |        0 |        36     |             0.442 |     0.264 |                       0.915 |      0.223 |      0.231 |     0.223 |     0.292 |     0.292 |     0.208 |     0.262 |     0.269 |
| mobility            | rl    |     2 |          19 |        0 |        47.368 |             0.48  |     0.358 |                       0.89  |      0.19  |      0.3   |     0.24  |     0.27  |     0.28  |     0.21  |     0.29  |     0.22  |
| mobility_hetero     | bolr  |     0 |          23 |        0 |        39.13  |             0.457 |   nan     |                       0.525 |      0.4   |      0.425 |     0.167 |     0.117 |     0.167 |     0.183 |     0.267 |     0.275 |
| mobility_hetero     | bolr  |     1 |          14 |        0 |        64.286 |             0.502 |   nan     |                       0.693 |      0.347 |      0.293 |     0.227 |     0.213 |     0.293 |     0.28  |     0.133 |     0.213 |
| mobility_hetero     | bolr  |     2 |          22 |        0 |        40.909 |             0.498 |   nan     |                       0.4   |      0.252 |      0.478 |     0.113 |     0.104 |     0.235 |     0.104 |     0.4   |     0.313 |
| mobility_hetero     | grpc  |     0 |          27 |        0 |        33.333 |             0.237 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| mobility_hetero     | grpc  |     1 |          24 |        0 |        37.5   |             0.208 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| mobility_hetero     | grpc  |     2 |          29 |        0 |        31.034 |             0.266 |   nan     |                       0     |      0     |      0     |     1     |     1     |     0     |     0     |     0     |     0     |
| mobility_hetero     | http3 |     0 |          39 |        0 |        23.077 |             0.936 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| mobility_hetero     | http3 |     1 |          35 |        0 |        25.714 |             0.894 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| mobility_hetero     | http3 |     2 |          40 |        0 |        22.5   |             1.007 |   nan     |                       0     |      1     |      1     |     0     |     0     |     0     |     0     |     0     |     0     |
| mobility_hetero     | rl    |     0 |          18 |        0 |        50     |             0.367 |     0.377 |                       0.895 |      0.2   |      0.211 |     0.221 |     0.295 |     0.326 |     0.221 |     0.253 |     0.274 |
| mobility_hetero     | rl    |     1 |          18 |        0 |        50     |             0.354 |     0.377 |                       0.916 |      0.189 |      0.284 |     0.232 |     0.284 |     0.295 |     0.221 |     0.284 |     0.211 |
| mobility_hetero     | rl    |     2 |          21 |        0 |        42.857 |             0.49  |     0.324 |                       0.9   |      0.209 |      0.318 |     0.236 |     0.255 |     0.282 |     0.218 |     0.273 |     0.209 |

## Switching cost: idle sessions across handovers (live selector runs, first use of each protocol excluded)

cold = the client has had a handover since it last used this protocol; stall = transfer > 25 s.

|                 |    n |   mean_s |   median_s |   stall_share |
|:----------------|-----:|---------:|-----------:|--------------:|
| ('QUIC', False) | 5291 |    2.868 |      1.104 |         0.011 |
| ('QUIC', True)  |  143 |   14.57  |      6.389 |         0.294 |
| ('TCP', False)  | 7536 |    5.481 |      2.09  |         0.025 |
| ('TCP', True)   |  418 |   21.685 |     10.368 |         0.392 |

