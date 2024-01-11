for ((a=1;a<=100;a++))
do
	echo "Term:$a"
	echo "--------------------------------------"
	#time go test -run 2A
	#time go test -run 2B
	#time go test -run 2C
	time go test -run TestFigure8Unreliable2C
	echo "--------------------------------------"
done
