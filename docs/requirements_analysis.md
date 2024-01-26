## High Level Requirements Analysis

**Target: Configurable Distributed computation framework.**

* Build a reusable library for querying large-scale data.
* Must allow developers to configure large-scale data, and query data at a specific grain, by executing computation tasks on the fly.


### Data
* Multiple high volume structured datasets.
* 10-15 columns per dataset
* On average, could have millions of rows of data
* Data sets are static for the day. Only appended on nightly basis.
* Streaming data is not currently a requirement.
* Data sets can be in csv, or other optimal formats.
 

#### Memory Requirement
* 15 cols * 5M rows * 250B * 10 datasets
* = 187GB

### Queries
* Made by the user to get information.
* Should be able to make multiple queries on the same set of data.
* Performs computational logic over single/multiple datasets.
* Should be able to specify row selection logic
* Should be able to specify column selection logic
* *Optional: User might want to specify if the query (generally for time consuming ones) should run in async mode.*
* Should be able to support on-the-fly expressions as columns
* Could be limited to just a single functional computation.
* Optimized for ease of usage. *At cost of performance due to inference*
* Functional Computation, by itself can also UDFs. (*User-Defined Functions (UDFs) are user-programmable routines that act on one row of data.*)

#### Samples
1. (date,symbol,ISIN,series,name_of_company,mean(volume),last_2_yr_high)
> *BreakDown:*
>* date, symbol used as the joining criteria.
>* ISIN,series,last_2_yr_high are column names, in different(or same) datasets.
>* mean(volume) : Functional Expression: General SQL function over volume (a dataset column).

2. (“series” “mean(volume)”, date>”20230312”, series=”EQ”)
>* series: column name in a dataset.
>* mean(volume) : Functional Expression: General SQL function over volume (a dataset column).
>* date>20230312, series=”EQ”: Conditional Expressions, used as a row selector. Interpreted as **AND** if multiple conditions.

3. Get latest previous close for all stocks whose face_value is 10
>* Query Data (prev_close, face_val>10)
>* prev_close: column name in a dataset.
>* **Question**: latest previous close would mean a groupBy on the symbol, and then selecting where face value > 0. How will this be represented in the Query as show? How does the library decide what to group by on?
Or, is the expectation here to make nested calls from client? 
4. Get timeseries of open for a given symbol for last 1 year
>* Query Data (open, date>01012023, date<01012024)
>* open: column name in a dataset.
>* date>01012023, date<01012024: Conditional Expressions, used as a row selector. Interpreted as **AND** if multiple conditions.

### Async Computes
T.B.D

### Authorization
T.B.D
