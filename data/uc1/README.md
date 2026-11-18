# UC1: in-vehicle thermal-comfort data

`uc1_thermal_comfort.csv` contains 2115 samples recorded in a passenger car with three drivers (driver codes 111, 222
and 555). Each sample combines cabin and ambient conditions, the skin temperature and relative humidity measured in
each seat zone, and the thermal-comfort votes (predicted mean vote scale, PMV) given by the occupants. The FedAvg
experiment in `analysis/fedavg_uc1.py` predicts the driver's vote (`Zone1 PMV (driver)`) from six inputs.

The file contains no personal information. The driver codes are pseudonyms, and the recording time is given as
seconds since the first sample of the respective driver (`t_s`) instead of a date and clock time. The rows are stored
in recording order.

| Column | Unit | Description |
|---|---|---|
| `driver_id` | - | pseudonymous driver code |
| `t_s` | s | time since the driver's first sample |
| `Ambient Temperature` | °C | outside air temperature |
| `Cabin Temperature` | °C | cabin air temperature |
| `Relative Humidity` | % | cabin relative humidity |
| `Solar Load` | - | solar load sensor reading |
| `Metabolic rate` | met | metabolic rate assumed for the occupants |
| `Clothing insulation` | clo | clothing insulation |
| `Zone1 skin Temperature`, `Zone1 RH` | °C, % | skin temperature and relative humidity, driver zone |
| `Zone2 skin Temperature`, `Zone2 RH` | °C, % | co-driver zone |
| `Zone3 skin Temperature`, `Zone3 RH` | °C, % | rear zone |
| `Zone1 PMV (driver)` | - | thermal-comfort vote of the driver |
| `Zone2 PMV (co-pas)` | - | vote of the co-driver |
| `Zone3_L PMV`, `Zone3_R PMV` | - | votes of the rear passengers, left and right (empty where no vote was recorded) |

License: Creative Commons Attribution 4.0 International (see `data/LICENSE`).
