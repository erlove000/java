<?php
include('includes/config.php');
$res = mysqli_query($con, "SHOW COLUMNS FROM ulb_user_profile");
$cols = [];
while($row = mysqli_fetch_assoc($res)) {
    $cols[] = $row['Field'];
}

$mapped = [];
$res2 = mysqli_query($con, "SELECT mapped_column FROM survey_questions WHERE mapped_column IS NOT NULL");
while($row = mysqli_fetch_assoc($res2)) {
    $mapped[] = $row['mapped_column'];
}

$ignore = ['id', 'town_id', 'district_id', 'updated_by', 'status', 'created_at', 'updated_at'];
$missing = array_diff($cols, $mapped, $ignore);

echo "Missing Columns:<br>";
foreach($missing as $m) {
    echo "- $m <br>";
}
?>
