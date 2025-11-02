Node.js v22.21.0
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % export CP_BASE=https://localhost:5000
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % export PORT=8080
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % export IB_HOST=127.0.0.1
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % export IB_PORT=5000
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % export IB_SSL=1
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % export IB_COOKIE='0.310c3417.1762060924.43705beb'
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % export IB_ALLOW_INSECURE=1 
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % export CP_INSECURE=1
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % export CP_CA_FILE=/Users/manojkandlikar/Downloads/ClientPortalWebAPI.pem
(base) manojkandlikar@Manojs-MacBook-Pro tradeflashflow % node index.js                                                           
HTTP+WS @ :8080  MOCK=off  IB=https://127.0.0.1:5000/v1/api

Last login: Sat Nov  1 21:55:50 on ttys021
(base) manojkandlikar@Manojs-MacBook-Pro ibkrtradeflow % /Users/manojkandlikar/Downloads/clientportal.gw/bin/run.sh 
(base) manojkandlikar@Manojs-MacBook-Pro ibkrtradeflow % node index.js
(base) manojkandlikar@Manojs-MacBook-Pro ibkrtradeflow % 
(base) manojkandlikar@Manojs-MacBook-Pro ibkrtradeflow % cd /Users/manojkandlikar/Downloads/clientportal.gw
(base) manojkandlikar@Manojs-MacBook-Pro clientportal.gw % ls
bin	build	dist	doc	root
(base) manojkandlikar@Manojs-MacBook-Pro clientportal.gw % find . --name "/Users/manojkandlikar/Downloads/clientportal.gw"
(base) manojkandlikar@Manojs-MacBook-Pro clientportal.gw % find . --name "conf.yaml"
find: --name: unknown primary or operator
(base) manojkandlikar@Manojs-MacBook-Pro clientportal.gw % find . -name "conf.yaml" 
./root/conf.yaml
(base) manojkandlikar@Manojs-MacBook-Pro clientportal.gw % xbin
(base) manojkandlikar@Manojs-MacBook-Pro clientportal.gw % 
(base) manojkandlikar@Manojs-MacBook-Pro clientportal.gw % bin/run.sh root/conf.yaml 
running  
 runtime path : root:dist/ibgroup.web.core.iblink.router.clientportal.gw.jar:build/lib/runtime/*
 config file  : root/conf.yaml
 -> mount demo on /demo
Java Version: 1.8.0_471
****************************************************
version: a27ed42161ad96c53e715ca5c5e3e3fa4cff5262 Mon, 24 Apr 2023 15:41:53 -0400
****************************************************
This is the Client Portal Gateway
for any issues, please contact api@ibkr.com
and include a copy of your logs
****************************************************
https://www.interactivebrokers.com/api/doc.html
****************************************************
Open https://localhost:5000 to login
App demo is available after you login under: https://localhost:5000/demo#/

