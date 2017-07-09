#!/bin/bash
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles17.xml-p011539268p013693066.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles18.xml-p013693075p016120541.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles19.xml-p016120548p018754723.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles20.xml-p018754736p021222156.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles21.xml-p021222161p023927980.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles22.xml-p023927984p026823658.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles23.xml-p026823661p030503448.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles24.xml-p030503454p033952815.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles25.xml-p033952817p038067198.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles26.xml-p038067204p042663461.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles27.xml-p042663464p053249142.bz2

bzip2 -d enwiki-20170220-pages-articles17.xml-p011539268p013693066.bz2
bzip2 -d enwiki-20170220-pages-articles18.xml-p013693075p016120541.bz2
bzip2 -d enwiki-20170220-pages-articles19.xml-p016120548p018754723.bz2
bzip2 -d enwiki-20170220-pages-articles20.xml-p018754736p021222156.bz2
bzip2 -d enwiki-20170220-pages-articles21.xml-p021222161p023927980.bz2 
bzip2 -d enwiki-20170220-pages-articles22.xml-p023927984p026823658.bz2
bzip2 -d enwiki-20170220-pages-articles23.xml-p026823661p030503448.bz2
bzip2 -d enwiki-20170220-pages-articles24.xml-p030503454p033952815.bz2
bzip2 -d enwiki-20170220-pages-articles25.xml-p033952817p038067198.bz2
bzip2 -d enwiki-20170220-pages-articles26.xml-p038067204p042663461.bz2
bzip2 -d enwiki-20170220-pages-articles27.xml-p042663464p053249142.bz2 

mv enwiki-20170220-pages-articles17.xml-p011539268p013693066 articles17.xml
mv enwiki-20170220-pages-articles18.xml-p013693075p016120541 articles18.xml
mv enwiki-20170220-pages-articles19.xml-p016120548p018754723 articles19.xml
mv enwiki-20170220-pages-articles20.xml-p018754736p021222156 articles20.xml
mv enwiki-20170220-pages-articles21.xml-p021222161p023927980 articles21.xml
mv enwiki-20170220-pages-articles22.xml-p023927984p026823658 articles22.xml
mv enwiki-20170220-pages-articles23.xml-p026823661p030503448 articles23.xml
mv enwiki-20170220-pages-articles24.xml-p030503454p033952815 articles24.xml
mv enwiki-20170220-pages-articles25.xml-p033952817p038067198 articles25.xml
mv enwiki-20170220-pages-articles26.xml-p038067204p042663461 articles26.xml
mv enwiki-20170220-pages-articles27.xml-p042663464p053249142 articles27.xml

