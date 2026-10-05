<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);
include('includes/config.php');

$target_town_id = 1;
$mapped_cols = ['total_properties' => 10];
$insert_sql = "INSERT INTO ulb_user_profile SET town_id=$target_town_id";

foreach($mapped_cols as $col => $val) {
    if($col === 'revenue_breakdown_json' && is_array($val)) {
        $val = json_encode($val);
    } else if(is_array($val)) {
        $val = implode(", ", $val);
    }
    $clean_val = mysqli_real_escape_string($con, $val);
    $insert_sql .= ", `$col`='$clean_val'";
}

echo "SQL: $insert_sql <br>";
if($con->query($insert_sql)) {
    echo "Success!";
} else {
    echo "Error: " . $con->error;
}
?>
