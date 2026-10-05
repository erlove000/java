<?php
$ch = curl_init();
curl_setopt($ch, CURLOPT_URL,"http://localhost/streetlight/user_profile.php");
curl_setopt($ch, CURLOPT_POST, 1);
curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query(['save_profile' => '1', 'selected_town_id' => '1', 'ans' => ['1' => 'Test']]));
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
$server_output = curl_exec($ch);
curl_close($ch);
echo "Response Length: " . strlen($server_output) . "\n";
if(strpos($server_output, 'ULB Profile saved successfully') !== false) {
    echo "Success Message Found!";
} else if (strpos($server_output, 'Database Error') !== false) {
    echo "DB Error Found!";
} else if (strpos($server_output, 'Fatal error') !== false) {
    echo "Fatal Error Found!";
} else {
    echo "No relevant message found.";
}
?>
