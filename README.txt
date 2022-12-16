Originally by Sergey Kirillov (https://github.com/pistolero/zbxsend),
further modified (Python 3 and other changes) by Markku Leiniö in 2019
and later.

=====
Usage
=====

Sample usage:::

    >>> from zbxsend import Metric, send_to_zabbix    
    >>> send_to_zabbix([Metric('localhost', 'bucks_earned', 99999)], 'localhost', 10051)
