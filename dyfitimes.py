#! /bin/env python3

import json
from datetime import datetime
from DyfiMysql import Db

eventsfile='allevents_2014.json'
entriesfile='entries_2014.json'
saveentries=False
maxtime=20*60 # In seconds
eventsearchtext='year(eventdatetime)=2014 or year(eventdatetime)=2015'

allentries={}
thisevent=None

# Get set of events

def getlist(eventsfile):
    results=None
    try:
        f=open(eventsfile,'r')
        results=json.load(f)
        print('Loaded',eventsfile,'with',len(results['results']),'results.')

    except IOError:
        db=Db()
        text=eventsearchtext
        text+=' and nresponses>=5'
        text+=' and (invisible=0 or invisible is null)'

        results=db.query(table='event',text=text)
        results={
                'query':text,
                'results':results,
                'query':eventsearchtext
                }

        with open(eventsfile,'w') as f:
            print('Writing to',eventsfile)
            f.write(json.dumps(results,indent=4,default=Db.serialize_datetime))

    return results


def getEntryTimes(evid):
    global allentries
    global saveentries 

    if len(allentries)==0:
        try:
            f=open(entriesfile,'r')
            allentries=json.load(f)
            print('Loaded',entriesfile,'with',len(allentries),'results.')
        except FileNotFoundError:
            print('No file found, creating',entriesfile)

    if evid in allentries:
        return allentries[evid]['times']

    db=Db()
    saveentries=True

    # Right now, every entry is from 2014+.
    # TODO: Specify earliest extended table.
    text='eventid="%s"' % (evid)
    results=db.query(table='extended',text=text)

    times=parsetimes(results)
    filteredtimes=[t for t in times if t<=maxtime]
    return filteredtimes


def parsetimes(results):
    times=[]
    evdate=thisevent['eventdatetime']
    if isinstance(evdate,str):
        evdate=datetime.strptime(evdate,'%Y-%m-%dT%H:%M:%S')

    for entry in results:
        entrydate=entry['time_now']
        if isinstance(entrydate,str):
            entrydate=datetime.strptime(evdate,'%Y-%m-%dT%H:%M:%S')

        timediff=(entrydate-evdate).total_seconds()
        times.append(timediff)

    return times


def main():
    global thisevent

    events=getlist(eventsfile)
    events=events['results']

    # Iterate through events

    eventtimes={}
    print('Iterating through',len(events),'events.')
    counter=0;
    for event in events:

        thisevent=event
        evid=event['eventid']
        counter+=1;

        # Filter out events here
        if not event['nresponses']:
            continue
        if event['nresponses']<10:
            continue
        if event['invisible']:
            continue
        
        if counter%100==0:
            print('%s: Reading event %s.' % (counter,evid))

        # Find entry times
        times=getEntryTimes(evid)

        if evid not in allentries:
            print('Saving',evid,'to allentries.')

            eventdata={
                    'times':times,
                    'magnitude':event['mag'],
                    'region':event['region'],
                    'nresponses':event['nresponses']
                    }
            allentries[evid]=eventdata

    # allentries should be populated at this point

    if saveentries:
        with open(entriesfile,'w') as f:
            print('Writing to',entriesfile)
            f.write(json.dumps(allentries,indent=4,default=Db.serialize_datetime))


if __name__=='__main__':
    main()

