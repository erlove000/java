<?php
include('includes/config.php');

// Find category ID for Finance & Accounts
$res = mysqli_query($con, "SELECT id FROM survey_categories WHERE category_name = 'Finance & Accounts'");
$cat = mysqli_fetch_assoc($res);
$cat_id = $cat['id'];

// Insert the missing question
mysqli_query($con, "INSERT INTO survey_questions (category_id, question_text, input_type, is_required, display_order, mapped_column) VALUES ($cat_id, 'Revenue Breakdown Details (JSON/Text)', 'textarea', 0, 14, 'revenue_breakdown_json')");

echo "Missing question added!";
?>
