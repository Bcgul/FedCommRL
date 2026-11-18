# FedAvg on UC1: IID vs non-IID, lost updates

Target R2 = 0.847 (95 % of IID median final R2)

|                       |   r2_final |   r2_bad_driver |   rounds_to_target | reached   |   n |   round_time_s |   time_to_target_min |
|:----------------------|-----------:|----------------:|-------------------:|:----------|----:|---------------:|---------------------:|
| ('IID', 'AMQP')       |      0.887 |           0.873 |                 74 | 98%       |  50 |          inf   |              inf     |
| ('IID', 'HTTP/3')     |      0.891 |           0.885 |                 59 | 90%       |  10 |           39.1 |               38.448 |
| ('IID', 'MQTT')       |      0.889 |           0.876 |                 70 | 98%       |  50 |          110.1 |              128.45  |
| ('IID', 'gRPC')       |      0.888 |           0.877 |                 70 | 98%       |  50 |           77   |               89.833 |
| ('non-IID', 'AMQP')   |      0.518 |           0.287 |                nan | 0%        |  50 |          inf   |              nan     |
| ('non-IID', 'HTTP/3') |      0.621 |           0.616 |                nan | 0%        |  10 |           39.1 |              nan     |
| ('non-IID', 'MQTT')   |      0.61  |           0.607 |                nan | 0%        |  50 |          110.1 |              nan     |
| ('non-IID', 'gRPC')   |      0.612 |           0.611 |                nan | 0%        |  50 |           77   |              nan     |


non-IID, driver 555 on the bad link (median over seeds):

| protocol   |   r2_final |   r2_bad_driver |
|:-----------|-----------:|----------------:|
| AMQP       |      0.473 |          -0.938 |
| MQTT       |      0.614 |           0.667 |
| gRPC       |      0.601 |           0.683 |


IID (median over seeds and bad clients):

| protocol   |   r2_final |   r2_bad_driver |
|:-----------|-----------:|----------------:|
| AMQP       |      0.887 |           0.873 |
| HTTP/3     |      0.891 |           0.885 |
| MQTT       |      0.889 |           0.876 |
| gRPC       |      0.888 |           0.877 |
