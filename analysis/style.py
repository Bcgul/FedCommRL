"""Shared colours and matplotlib settings for all figures.

Every protocol and every selector keeps the same colour in all figures. The palette is dark enough for print and
separates red and green by lightness, so that it remains readable with red-green colour vision deficiency.
"""
PROTO_COLOR = {"http3": "#08519c", "grpc": "#a50f15", "mqtt": "#35a35a", "amqp": "#6a51a3",
               "quic": "#08519c", "quinn": "#4292c6"}
ARM_COLOR = {"http3": PROTO_COLOR["http3"], "grpc": PROTO_COLOR["grpc"], "mqtt": PROTO_COLOR["mqtt"],
             "amqp": PROTO_COLOR["amqp"], "bolr": "#c45a00", "rl": "#000000"}
PROTO_NAME = {"http3": "HTTP/3", "grpc": "gRPC", "mqtt": "MQTT", "amqp": "AMQP"}
NAME_COLOR = {PROTO_NAME[k]: v for k, v in PROTO_COLOR.items() if k in PROTO_NAME}


def setup(plt, size=8):
    plt.rcParams.update({"font.size": size, "axes.labelsize": size, "axes.titlesize": size,
                         "xtick.labelsize": size - 1, "ytick.labelsize": size - 1, "legend.fontsize": size - 1,
                         "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
                         "grid.color": "#d9d9d9", "grid.linewidth": 0.5, "axes.edgecolor": "#333333",
                         "pdf.fonttype": 42, "ps.fonttype": 42})
