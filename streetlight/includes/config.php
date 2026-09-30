<?php
  define('DB_SERVER', '10.43.29.164');
  define('DB_USERNAME', 'pmidcpb');
  define('DB_PASSWORD', 'Ra2@bV#!2X');
  define('DB_DATABASE', 'pmidc-mis1');
$con =  mysqli_connect(DB_SERVER,DB_USERNAME,DB_PASSWORD,DB_DATABASE);
// Check connection
if (mysqli_connect_errno())
{
 echo "Failed to connect to MySQL: " . mysqli_connect_error();
}
?>
