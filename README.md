# CPS State Replication

CPS State Replication is an implementation of the replication mode of [CPS Twinning](https://github.com/sbaresearch/cps-twinning), a framework for generating and executing digital twins. When running CPS Twinning in this particular mode, the digital twins follow the states of their physical counterparts by passively monitoring stimuli. In this way, a variety of use cases can be realized. For instance, an intrusion detection technique can be implemented by comparing the inputs and outputs of the digital twins to those of the real devices.

## Installation

First, install [Scala](https://www.scala-lang.org/download/). Then, set the path to the AML file and the Kafka connection settings in the [configuration file](src/main/resources/application.conf). An exemplary specification in AML can be found in the [CPS Twinning](https://github.com/sbaresearch/cps-twinning/blob/master/misc/specification/CandyFactory.aml) repository. Finally, build the application with sbt.

## Disclaimer

Note that this project is only a proof of concept. As a consequence, there are currently many areas that need improvements. In particular, the functionality of the AutomationML parser is currently limited and may require manual adjustments.