GHC=ghc -O2 -rtsopts -threaded -with-rtsopts "-A10m -N"

PROF=-prof -auto-all

CBITS=cbits/rollsum.c cbits/term.c


main: $(dir *.hs *.c)
	${GHC} Main ${CBITS} -o ../bin/hindsight


main-prof: $(dir *.hs *.c)
	${GHC} ${PROF} Main ${CBITS} -o ../bin/hindsight-prof


clean:
	find . -iname '*.o' -exec rm {} \;
	find . -iname '*.hi' -exec rm {} \;
