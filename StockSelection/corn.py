from crontab import CronTab

wd="/Users/ajaysinghrajawat/PycharmProjects/techno-trade/StockSelection"
cron = CronTab('ajaysinghrajawat')
#
job = cron.new(command='cd //Users/ajaysinghrajawat/PycharmProjects/techno-trade/StockSelection && ./utility.py >> //Users/ajaysinghrajawat/PycharmProjects/techno-trade/StockSelection/logs/cron.log 2>&1')
job.minute.every(45)

cron.write()
print ("Job created")

# list all cron jobs (including disabled ones)

#cron.remove_all()

for job in cron:
     print (job)



# list all cron jobs (including disabled ones)
