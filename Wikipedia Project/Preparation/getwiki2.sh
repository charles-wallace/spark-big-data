#!/bin/bash
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles9.xml-p001791081p002336422.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles10.xml-p002336425p003046511.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles11.xml-p003046517p003926861.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles12.xml-p003926864p005040435.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles13.xml-p005040438p006197593.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles14.xml-p006197599p007744799.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles15.xml-p007744803p009518046.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles16.xml-p009518059p011539266.bz2

bzip2 -d enwiki-20170220-pages-articles9.xml-p001791081p002336422.bz2
bzip2 -d enwiki-20170220-pages-articles10.xml-p002336425p003046511.bz2
bzip2 -d enwiki-20170220-pages-articles11.xml-p003046517p003926861.bz2
bzip2 -d enwiki-20170220-pages-articles12.xml-p003926864p005040435.bz2
bzip2 -d enwiki-20170220-pages-articles13.xml-p005040438p006197593.bz2
bzip2 -d enwiki-20170220-pages-articles14.xml-p006197599p007744799.bz2
bzip2 -d enwiki-20170220-pages-articles15.xml-p007744803p009518046.bz2
bzip2 -d enwiki-20170220-pages-articles16.xml-p009518059p011539266.bz2

mv enwiki-20170220-pages-articles9.xml-p001791081p002336422 articles9.xml
mv enwiki-20170220-pages-articles10.xml-p002336425p003046511 articles10.xml
mv enwiki-20170220-pages-articles11.xml-p003046517p003926861 articles11.xml
mv enwiki-20170220-pages-articles12.xml-p003926864p005040435 articles12.xml
mv enwiki-20170220-pages-articles13.xml-p005040438p006197593 articles13.xml
mv enwiki-20170220-pages-articles14.xml-p006197599p007744799 articles14.xml
mv enwiki-20170220-pages-articles15.xml-p007744803p009518046 articles15.xml
mv enwiki-20170220-pages-articles16.xml-p009518059p011539266 articles16.xml