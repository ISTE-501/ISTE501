
<h2>Student Data:</h2>
<?php 
//we are connecting to database
	include($path . "connect.php");
	//query to show all exisitng data we can group by the column 1 being student id and column 2 being class id
	$query = "SELECT * FROM tablename WHERE condition GROUP BY column1, column2;";
	
	$result= $mysqli->query($query);
	
	while($row = $result->fetch_assoc())
	{
		//var_dump($row);
	//keep fetching until end of data in our db	
	
		echo '<div class="row1">'.$row['rowname'].'</div>';
		
		
	
	}
	
	?>