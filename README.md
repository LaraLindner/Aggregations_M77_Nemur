# Projekt Dokumentation

## Name
M77_Nemur

## Übersicht
In diesem Projekt sind sämtliche MongoDB-Aggregationen als Python-Skripte für den Tierversuch M77_Nemur enthalten. Die zugrunde liegenden Daten stammen aus der VSB-Datenbank und wurden themenspezifisch aus verschiedenen Collections in MongoDB aggregiert und aufbereitet.

Die Skripte sind so konzipiert, dass sie beispielsweise über Cronjobs automatisiert ausgeführt werden können – etwa zur täglichen Zusammenstellung relevanter Daten. Im Rahmen der Aggregation werden die aufbereiteten Daten in separate Ziel-Collections innerhalb der MongoDB-Datenbank geschrieben. Von dort aus können sie zentral eingesehen und bei Bedarf weiterverarbeitet oder exportiert werden.

## Verwendung
Die Skripte lassen sich direkt über die Konsole oder automatisiert via Cronjob ausführen. Eine manuelle Anpassung ist in der Regel nicht erforderlich.

Wichtig! 
Sollten die neu erzeugten Collections auch extern abrufbar sein müssen, setzen Sie sich bitte mit dem Support in Verbindung. In diesem Fall muss das entsprechende Zugriffs-Schema auf Seiten der MongoDB angepasst werden.


## Support
AG-Daten-BSW@fli.de


## Autoren
Lara Lindner


## Project status
Fertig


## Overview
This project contains all MongoDB aggregation operations as Python scripts for the M77_Nemur animal experiment. The underlying data originates from the VSB database and has been aggregated and processed from various MongoDB collections based on specific topics.

The scripts are designed to be executed automatically, for example via cronjobs, to perform regular data aggregation tasks—such as daily compilation of relevant information. During execution, the aggregated data is written into dedicated target collections within the MongoDB database. From there, the data can be centrally accessed, viewed, and further processed or exported as needed.

## Usage
The scripts can be executed directly from the command line or scheduled via cronjob. No manual adjustments are typically required.

Importent:
If the newly generated collections need to be accessible from external sources, please contact the support team. In such cases, the corresponding MongoDB access schema needs to be adjusted accordingly.

## Support
AG-Daten-BSW@fli.de

## Authors
Fabian Billenkamp

## Project Status
Completed