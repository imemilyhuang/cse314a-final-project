# cse314a-final-project

Quick note: Initially, we wrote and tested our code locally to prototype the scraping mechanism. However, when we were automating the code on Airflow, we ran into a ton of issues with Selenium ChromeDriver (what we used locally to scrape the data). ChromeDriver doesn't work in Airflow by default because the environment running Airflow (like a Docker container or remote server) usually doesn't have Google Chrome installed or doesn't have the graphical system libraries that ChromeDriver needs to open a browser. We also didn't have admin privileges to install Chrome and ChromeDriver inside the Airflow environment. Without a real Chrome browser and the necessary system support, ChromeDriver can't start.

We ended up finding these solutions documented below: 
- https://www.youtube.com/watch?v=PA7TxBa-XDU
- https://stackoverflow.com/questions/75842155/how-to-use-selenium-with-chromedriver-in-apache-airflow-in-docker

However, it can be extremely finicky at times and may require you to manually install Docker, change the Selenium documentation in the docker-compose.yaml file, and other additional add-ons/changes depending on the type of computer you're using. In case this doesn't work out for you, we've also included the code we wrote locally so you can test out the scraping function with Selenium ChromeDriver (located in the folder "Local Code").
