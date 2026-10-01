<?php
if (!defined('DB_SERVER')) define('DB_SERVER', '');
if (!defined('DB_USERNAME')) define('DB_USERNAME', '');
if (!defined('DB_PASSWORD')) define('DB_PASSWORD', '');
if (!defined('DB_DATABASE')) define('DB_DATABASE', '');

if (!isset($con) || !$con) {
  $con = mysqli_connect(DB_SERVER, DB_USERNAME, DB_PASSWORD, DB_DATABASE);
  if (mysqli_connect_errno()) {
    error_log("Failed to connect to MySQL: " . mysqli_connect_error());
  }
}
?>
