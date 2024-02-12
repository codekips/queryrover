# Project Rover
## Usage:
* Download this git repo, and install packages using setuptools

### Query Management:
* Instantiate RoverMgmt with RoverMgt()
* Add datasets, using the add_dataset function. Usage below

1. rover_mgmt.add_dataset(name="small_file", location="tests/dumps/small_file.csv").
2. rover_mgmt.add_dataset(name="small_student", location="tests/dumps/small_student.csv")

-- name, location are mandatory params.
-- expected for csv to also have a header.


### Query Rover:

* Use Query Rover to now query for data

1. rover = QueryRover("")
2. q = rover.fetch(['family','product']).where('family','==','\'ProSeries\'');
3. q.compute()
The response of q.compute() is a pandas dataframe.
