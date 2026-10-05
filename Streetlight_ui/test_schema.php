<?php
include('includes/config.php');
$res = mysqli_query($con, "SHOW COLUMNS FROM ulb_user_profile");
while($row = mysqli_fetch_assoc($res)) {
    echo $row['Field'] . "<br>";
}
?>
