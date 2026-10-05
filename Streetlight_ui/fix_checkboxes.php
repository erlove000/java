<?php
include('includes/config.php');

// 1. Add options column
mysqli_query($con, "ALTER TABLE survey_questions ADD COLUMN options_text TEXT DEFAULT NULL");

// 2. Update Q9 (ulb_known_for)
$q9_options = "Administrative centre, Industrial activity, Agriculture or mandi-related activity, Trade and commercial activity, Tourism or heritage, Religious importance, Educational institutions, Healthcare facilities, Transport or logistics, Residential or satellite town, Other";
mysqli_query($con, "UPDATE survey_questions SET input_type='checkbox', options_text='".mysqli_real_escape_string($con, $q9_options)."' WHERE mapped_column='ulb_known_for'");

// 3. Update Q10 (top_complaint_services)
$q10_options = "Water supply, Sewerage, Solid waste collection, Street sweeping, Roads, Streetlights, Drainage and waterlogging, Parking, Public toilets, Parks, Property tax, Building approvals, Encroachment, Stray animals, Other";
mysqli_query($con, "UPDATE survey_questions SET input_type='checkbox', options_text='".mysqli_real_escape_string($con, $q10_options)."' WHERE mapped_column='top_complaint_services'");

echo "DB updated for checkboxes!";
?>
