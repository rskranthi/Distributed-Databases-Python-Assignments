equijoin is the Driver Class
JoinMapper is the Mapper which splits the values as key and the whole string is sent as value to the reducer phase
JoinReducer is the Reducer which iterates over the reducers and concatenates the strings if the Table Names are not same

Package Name is Join and the Join.equijoin may be used to run code if equijoin as class name does not work
