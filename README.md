# Car sharing simulator

This simulator is a trace-drive simulator which relies on the real data coming from Car2go, a car sharing provider working in 25 cities spread around the world.
Please notice that the code is available, but the data are subjected to some restrictions. If you are interested, please contact: michele.cocca@polito.it


# Simulator structure
The simulator uses the users' data to:
- build the operative area
- build the trace, as a sequence of consecutive car rentals

The trace is a an array of events indexed by timestamp. Each events could be a rental begin or a rental end. According to the event type, the simulator change the status of the car from booked to parked or viceversa.

In this model we want approximate the user's behaviour if in the entire fleet is converted from combustion egninge to electric vehicles, placing a variable number of electric charging stations.


## Create the basic structures
Execute the `InitializeFoldersandInputs.sh`  passing as parameter the a city present in the `input/car2go_oper_areas_limits.csv` file.

## Run the simulation
- `RunningConfiguration/Parallel_Simulation.py` to run simulation without a given number of plugs
- `RunningConfiguration/Parallel_Simulation_ACS_Z.py` to run simulation with a given number of plugs

## Output
- Number of Infeasible trips
- Reroute percentage: How many times the user is oblid by the sistem to leave the car in a charging station
- Recharge percentage: How many times the user physically plugs the car
- Average e median walked distance: how much an user have to walk each time he is rerouted
- Average and median battery State of Charge (SOC)
- Average and median time spent in a charging station
- Percentage of users willingness
