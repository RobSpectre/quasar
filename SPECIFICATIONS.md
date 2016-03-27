### Living Document of Specifications for Quasar Platform

* Programming Languages for All Code and Scripts: Python 3, Coffee Script, or Node.js
* Operating System: Ubuntu LTS (Currently 14.04, and 16.04 as soon as tested on)
* Web Server and Load-Balancer: Nginx Mainline (Currently 1.9 Release)
* CI/Build/Enterprise Job Scheduler: Jenkins
* Automation and Configuration Management: Ansible Open-Source Version (Currently 2.x Release)
* Cloud Provider: Google Cloud
* Security Guidelines: Minimal Privilege, especially SSH. No direct Data Warehouse access
* BI/Visualization/Data Warehouse User-Access: Looker
* All Platform Access API Driven at all levels
* ChatOps C&C and Misc. Bot Needs: Hubot
* MySQL Based Data Storage: Percona
* On-going Documentation per Sprint
* All Infrastructure should be captured in code, wherever possible.
* Monitoring: New Relic, Slack Notifications, and ELK
* No other tools should be used for Quasar except the above, unless necessary and with proper vetting.

#### Local Development

* Vagrant
* Otto (To supercede Vagrant)

### Future Tool Considerations

* Docker. Useful across various potential points in platform, but obvious ones are ETL and API requests.
