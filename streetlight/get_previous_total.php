<?php
header('Content-Type: application/json');
error_reporting(0);
include_once('includes/config.php');

$serviceName = isset($_GET['service']) ? mysqli_real_escape_string($con, $_GET['service']) : '';
$townIdValue = isset($_GET['town_id']) ? (int)$_GET['town_id'] : 0;

if (!empty($serviceName) && $townIdValue > 0) {
  $query = "SELECT total_applications FROM pbtrac_report WHERE ulb = '$townIdValue' AND name_of_service = '$serviceName' ORDER BY date_of_reporting DESC LIMIT 1";
  $res = $con->query($query);
  if ($res && $res->num_rows > 0) {
    $row = $res->fetch_assoc();
    echo json_encode([
      'success' => true,
      'total_applications' => $row['total_applications']
    ]);
    exit();
  }
}

echo json_encode(['success' => false]);
exit();
?>
