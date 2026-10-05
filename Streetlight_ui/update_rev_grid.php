<?php
include('includes/config.php');
mysqli_query($con, "UPDATE survey_questions SET input_type='revenue_grid', question_text='How much revenue did the ULB collect from each of the following sources during the latest completed financial year?' WHERE mapped_column='revenue_breakdown_json'");
echo "Updated revenue grid!";
?>
