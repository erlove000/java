<?php
include('includes/config.php');
$res = $con->query("DESCRIBE towns_streetlight_mapping");
while($row = $res->fetch_assoc()){
    echo $row['Field'] . " - " . $row['Type'] . " - " . $row['Key'] . " - " . $row['Extra'] . "<br>\n";
}
?>
