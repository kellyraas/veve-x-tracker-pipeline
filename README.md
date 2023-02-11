# Veve X Tracker
The [Veve X Tracker](https://cutt.ly/vevextracker) is a dashboard built with Google Data Studio that tracks the transaction data of the VeVe Market. The aim of the Dashboard is to provide a transparent view of the VeVe Market to the Veve community and make the blockchain data accessible and easy to track and analyze. The Dashboard is updated on a daily basis and includes data about: Transfer Volume, Buyer & Sellers, Active wallets, New Active Wallets, Drop Details, Tokens minted as well as a section that tracks the transactions of the top 100 wallets. The data goes back to 01/01/2022.

## Data Source and Infrastructure
The dashboard is using blockchain data extracted via the [ImmutableX API](https://docs.x.immutable.com/reference/#/). The raw data is extracted once a day (01:00 UTC) and stored on AWS infrastructure (S3 Buckets). Another ETL running on Lambda estracts the data, transforms it and stores it in different tables in a relational database. 

## Veve X Tracker Pipeline
The [main.py](main.py) file in this repository is the pipeline that populates the data for the VeveXTracker Dashboard. The data is extracted from the relational database aggregated, formatted and then exported to different Google Sheets which are connected to Google Data Studio. The pipeline is currently executed manually, by myself every day (usually before 08:00 CET). 

![alt-text](/img/xtracker_infrastructure.png)

## About Veve
[VeVe](https://www.veve.me/) is an app-based marketplace that brings the world of collectibles into the digital realm. Featuring digital collectibles of iconic heroes, digital comics and limited digital artworks in premium digital format. VeVe digital collectibles are 3D sculptures thst can be viewed, displayed and interacted with through VR and AR. New sets are released each week into the VeVe Store through regular “Drops” and can be traded peer-to-peer in the secondary market. It uses Blockchain technology and runs on ImmutableX.

## Preview of the Dashboard
### Market Stats
![alt-text](/img/xtracker_view1.png)
### Top 100 Stats
![alt-text](/img/xtracker_view2.png)
