# Ship your rotated logs to Elastic Search
This script is only relevent to you if you use ELK stack to analyse your server logs. 

This script if for you if:
* You use Elastic Search(ES) for analysing your server logs
* For one of the reasons, the logs in your ES instance is missing or corrupted
    - Logs missing cases
        + Setup Elastic search log analytics after application server was live
        + Your log pipeline was down for some time
    - Logs currupted
        + You changed your logging format within your application server for some reason and you need to make your old logs compatable
            * added geo locations
            * added user

For one of the above reason or other, I typically end up having to re-index the application server logs on Elastic search every now and then. This script is to help simplify that process

## Basic defaults
This script can be modified to work with any rotated log files. This script is written with default assumption that you are using AWS Beanstalk on your application server. So the location and structure of storage of log rotated files assumes AWS defaults.
