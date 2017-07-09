#!/bin/bash
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles1.xml-p000000010p000030302.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles2.xml-p000030304p000088444.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles3.xml-p000088445p000200507.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles4.xml-p000200511p000352689.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles5.xml-p000352690p000565312.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles6.xml-p000565314p000892912.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles7.xml-p000892914p001268691.bz2
wget https://dumps.wikimedia.org/enwiki/20170220/enwiki-20170220-pages-articles8.xml-p001268693p001791079.bz2

bzip2 -d enwiki-20170220-pages-articles1.xml-p000000010p000030302.bz2
bzip2 -d enwiki-20170220-pages-articles2.xml-p000030304p000088444.bz2
bzip2 -d enwiki-20170220-pages-articles3.xml-p000088445p000200507.bz2
bzip2 -d enwiki-20170220-pages-articles4.xml-p000200511p000352689.bz2
bzip2 -d enwiki-20170220-pages-articles5.xml-p000352690p000565312.bz2
bzip2 -d enwiki-20170220-pages-articles6.xml-p000565314p000892912.bz2
bzip2 -d enwiki-20170220-pages-articles7.xml-p000892914p001268691.bz2
bzip2 -d enwiki-20170220-pages-articles8.xml-p001268693p001791079.bz2

mv enwiki-20170220-pages-articles1.xml-p000000010p000030302 articles1.xml
mv enwiki-20170220-pages-articles2.xml-p000030304p000088444 articles2.xml
mv enwiki-20170220-pages-articles3.xml-p000088445p000200507 articles3.xml
mv enwiki-20170220-pages-articles4.xml-p000200511p000352689 articles4.xml
mv enwiki-20170220-pages-articles5.xml-p000352690p000565312 articles5.xml
mv enwiki-20170220-pages-articles6.xml-p000565314p000892912 articles6.xml
mv enwiki-20170220-pages-articles7.xml-p000892914p001268691 articles7.xml
mv enwiki-20170220-pages-articles8.xml-p001268693p001791079 articles8.xml