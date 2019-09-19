# SSP (Simple Stream Processor)

The Simple Stream Processor makes it possible for processes to process data and pass it around.

Every process is an operator and can be scaled.    
Operators consume and produce to a transport layer.  
The transport layer is in charge of managing streams of data.

SSP provides a framework for:
 - producing and consuming data;
 - storing the internal state of operators;
 - managing the operators.

Its main characteristics are:
 - The transport is abstracted. It could be in-memory, a file, Kafka, etc.
 - SSP provides you with libraries to gracefully manage sending out and receive data.
 - SSP provides you with libraries to gracefully manage internal state to your operators.
 - SSP provides you with a launcher to design how your processes communicate.
 - SSP provides you with a manager to manage fault-tolerance.

## TODO

The first things will be done in Go, but many libraries can be created for many languages.

 - [ ] Draft of `receive`.
 - [ ] Draft of `send`.
 - [ ] Draft of a launcher.
