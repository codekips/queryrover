# Project Rover
Usage:
Download this git repo, and install packages using setuptools

Query Management:
Instantiate RoverMgmt with RoverMgt()
Add datasets, using the add_dataset function. Usage below

rover_mgmt.add_dataset(name="small_file", location="tests/dumps/small_file.csv").
name, location are mandatory params
expected for csv to also have a header

Multiple datasets can be added in the same way    
rover_mgmt.add_dataset(name="small_student", location="tests/dumps/small_student.csv")

Use Query Rover to now query for data

rover = QueryRover("")
q = rover.fetch(['family','product']).where('family','==','\'ProSeries\'');
q.compute()
The response of q.compute() is a pandas dataframe.
