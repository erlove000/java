<?php
include('includes/config.php');

echo "<h1>Migrating Survey to Dynamic Engine</h1>";

// Clear existing
mysqli_query($con, "TRUNCATE TABLE survey_categories");
mysqli_query($con, "TRUNCATE TABLE survey_questions");

$html = file_get_contents('user_profile.php');

// Extract Tabs (Categories)
preg_match_all('/<div class="tab-pane[^>]*id="([^"]+)"[^>]*>\s*<div class="form-section-title">([^<]+)<\/div>/i', $html, $cat_matches, PREG_SET_ORDER);

$category_map = []; // DOM id => DB id
$order = 1;

foreach($cat_matches as $match) {
    $dom_id = $match[1];
    $cat_name = trim($match[2]);
    
    // Clean up "Section X: " prefix
    $cat_name = preg_replace('/^Section\s*\d+:\s*/i', '', $cat_name);
    
    // Insert Category
    $stmt = $con->prepare("INSERT INTO survey_categories (category_name, display_order) VALUES (?, ?)");
    $stmt->bind_param("si", $cat_name, $order);
    $stmt->execute();
    $cat_id = $stmt->insert_id;
    
    $category_map[$dom_id] = $cat_id;
    $order++;
    
    echo "Added Category: $cat_name (ID: $cat_id)<br>";
}

// Extract Questions per Tab
preg_match_all('/<div class="tab-pane[^>]*id="([^"]+)"(.*?)<\/div>\s*<!-- TAB/is', $html . "<!-- TAB", $pane_matches, PREG_SET_ORDER);

$q_order = 1;
foreach($pane_matches as $pane) {
    $dom_id = $pane[1];
    if(!isset($category_map[$dom_id])) continue;
    $cat_id = $category_map[$dom_id];
    
    $pane_html = $pane[2];
    
    // Find all questions in this pane
    preg_match_all('/<label class="q-label">.*?<\/span>(.*?)<\/label>\s*<(input|select|textarea)[^>]*name="([^"]+)"[^>]*(type="([^"]+)")?[^>]*>/is', $pane_html, $q_matches, PREG_SET_ORDER);
    
    foreach($q_matches as $qm) {
        $q_text = trim(strip_tags($qm[1]));
        $is_req = (strpos($qm[1], '*') !== false) ? 1 : 0;
        
        $tag = strtolower($qm[2]);
        $name = $qm[3];
        $type_attr = isset($qm[5]) ? strtolower($qm[5]) : '';
        
        $input_type = 'text';
        if($tag == 'textarea') $input_type = 'textarea';
        else if($tag == 'select') $input_type = 'text'; // Fallback for dropdowns, we'll store as text for now
        else if($tag == 'input' && $type_attr == 'number') $input_type = 'number';
        else if($tag == 'input' && $type_attr == 'date') $input_type = 'date';
        
        // Skip hidden/system fields if any
        if($type_attr == 'hidden') continue;
        
        $stmt = $con->prepare("INSERT INTO survey_questions (category_id, question_text, input_type, is_required, display_order) VALUES (?, ?, ?, ?, ?)");
        $stmt->bind_param("issii", $cat_id, $q_text, $input_type, $is_req, $q_order);
        $stmt->execute();
        $q_order++;
        
        echo " - Added Q: $q_text ($input_type)<br>";
    }
}

echo "<h2>Migration Complete!</h2>";
?>
