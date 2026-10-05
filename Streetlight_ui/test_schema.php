<?php
include('includes/config.php');
$res = mysqli_query($con, "SHOW COLUMNS FROM towns_streetlight_mapping");
while($row = mysqli_fetch_assoc($res)) {
    echo $row['Field'] . " - " . $row['Type'] . " - " . $row['Key'] . " - " . $row['Extra'] . "<br>";
}
?>
