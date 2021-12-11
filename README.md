# Limit Order Book analysis application
Analyse the limit order book in seconds. Zoom to tick level or get yourself an overview of the trading day. Correlate the market activity with the Apple Keynote presentations.
## Demo
https://lob.physik.bayern

## Dissertation
**Modelling the short-term impact of live news-flows on the limit order book using an extended Hawkes process**
![University of Oxford](oxford-logo.png)

***Christ Church***

***University of Oxford***

*This application is part of the thesis submitted in partial fulfilment of the Master of Science in Mathematical Finance*

December 17, 2021
## Screenshot
![Animated Screenshot of the LOB App](app/demo.gif)

## Introduction
For screening LOB data and derived figures of our dissertation, and easy data discovery, we developed a web based graphical user interface. The application was optimised to show data on all time-levels, e.g., give an overview of the trading day, as well as, zoom into the data to view the impact of individual messages. Aggregation is done in real time using intelligent caching so that database requests and calculation effort are efficiently minimized. The user can choose different types of plots, such as 2D  heatmaps of the volume profile, the volume at touch, midprice, etc. Stock data are shown in sync with video data, as well as, speech data extracted from the audio file. The plots are interactive so that the user can zoom in and out of the data by clicking into the plots. Readers of our dissertation can use this application to get a overview of the analysed data and find their own results quickly.

# Requirements
* mongo-db database
* redit database
* to use OCR/speech recognition please provide Google cloud credentials (in config/google.json)
* to download LOB data, please provide TradingPhysics credentials (in config/tradingphyurl.txt)
* video files are automatically downloaded from Apple's podcast library
# Usage
Build container

    docker build -t lob:latest .
    
Run application

	docker run -d -it -p 8123:8123 --name lob -e REDIS_SERVER=redis -e MONGO_SERVER=mongo --network="db-network" -v /home/core/data:/data:ro --restart unless-stopped lob:latest python3 startup.py --nocheck

# Additional analysis libraries
Additional code that has been used for data analysis in our dissertation can be found here: https://github.com/LimitOrderBook/lob-analysis. 

Note that the analysis code is *not necessary* to run this application.

# Copyright

This app is is licensed under the MIT License (see LICENSE.TXT)
