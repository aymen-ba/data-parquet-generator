*Version séquentielle:

1500 ligne par fichier/ 100 MO par fichier / 4min par fichier ==> 100 fichiers

100 * 100 = 10 GO (Total)

100 * 4 = 400 min = 6.66 heure (pour 100 fichiers)

*version multithreading:

1500000 * 100 fichiers = 150 000 000 lignes

150000000 lignes / 8 threads = 18750000

run 15:42 ==> GCmemory out of range

=====================

/tmp/data/* ==> 100 fichiers (de 100 MO)  ===> 100 partitions ===> (--master Local[4])  ===> temps d'execution de job: 17min

/tmp/data/* ==> 100 fichiers (de 100 MO)  ===> 100 partitions ===> coalesce: 4 partitions ===> (--master Local[4])  ===> temps d'execution de job: java.lang.OutOfMemoryError: GC overhead limit exceeded

