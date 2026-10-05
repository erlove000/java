<?php
session_start();
error_reporting(E_ALL);
ini_set('display_errors', 1);
include('includes/config.php');

if (strlen($_SESSION['aid']) == 0) {
  header('location:index.php');
  exit();
}

// Fetch logged in user details
$sql_login = "SELECT districtid, townid, staff_name FROM streetlightlogin WHERE id=" . (int)$_SESSION['aid'];
$res_login = $con->query($sql_login);
$user_town_id = 0;
$user_dist_id = 0;
$staff_name = '';

if ($res_login && $res_login->num_rows > 0) {
  $row_l = $res_login->fetch_assoc();
  $user_town_id = $row_l['townid'];
  $user_dist_id = $row_l['districtid'];
  $staff_name = $row_l['staff_name'];
}

$ulb_name = "State Admin View";
$district_name = "All Districts";
if ($user_town_id > 0) {
  $sql_map = "SELECT town_name, district_name, distid FROM towns_streetlight_mapping WHERE town_id = " . (int)$user_town_id;
  $res_map = $con->query($sql_map);
  if ($res_map && $res_map->num_rows > 0) {
    $row_m = $res_map->fetch_assoc();
    $ulb_name = $row_m['town_name'];
    $district_name = $row_m['district_name'];
    $user_dist_id = $row_m['distid'];
  }
}

// Handle Form Submission
$msg = "";
$error = "";
$form_submitted_success = false;

if (isset($_POST['save_profile'])) {
  $target_town_id = ($user_town_id > 0) ? $user_town_id : (int)$_POST['selected_town_id'];
  
  if ($target_town_id <= 0) {
    $error = "Please select a valid ULB to submit profile information.";
  } else {
    // Get target ULB details
    $target_ulb_name = $ulb_name;
    $target_district_name = $district_name;
    $target_dist_id = $user_dist_id;
    if ($target_town_id != $user_town_id) {
      $res_target = $con->query("SELECT town_name, district_name, distid FROM towns_streetlight_mapping WHERE town_id = $target_town_id");
      if ($res_target && $res_target->num_rows > 0) {
        $row_t = $res_target->fetch_assoc();
        $target_ulb_name = $row_t['town_name'];
        $target_district_name = $row_t['district_name'];
        $target_dist_id = $row_t['distid'];
      }
    }

    $mapped_cols = [];
    $dyn_answers = [];
    
    if(isset($_POST['ans']) && is_array($_POST['ans'])) {
        foreach($_POST['ans'] as $q_id => $val) {
            $q_res = $con->query("SELECT mapped_column FROM survey_questions WHERE id=".(int)$q_id);
            if($q_row = $q_res->fetch_assoc()) {
                if(!empty($q_row['mapped_column'])) {
                    $mapped_cols[$q_row['mapped_column']] = $val;
                } else {
                    $dyn_answers[$q_id] = $val;
                }
            }
        }
    }

    $insert_sql = "INSERT INTO ulb_user_profile SET town_id=$target_town_id, district_id=$target_dist_id, updated_by='" . mysqli_real_escape_string($con, $staff_name) . "', status='Submitted', created_at=NOW()";
    
    $update_parts = [];
    foreach($mapped_cols as $col => $val) {
        if($col === 'revenue_breakdown_json' && is_array($val)) {
            $val = json_encode($val);
        } else if(is_array($val)) {
            $val = implode(", ", $val);
        }
        $clean_val = mysqli_real_escape_string($con, $val);
        if($clean_val === '') {
            $insert_sql .= ", `$col`=NULL";
            $update_parts[] = "`$col`=NULL";
        } else {
            $insert_sql .= ", `$col`='$clean_val'";
            $update_parts[] = "`$col`='$clean_val'";
        }
    }
    
    $insert_sql .= " ON DUPLICATE KEY UPDATE updated_by='" . mysqli_real_escape_string($con, $staff_name) . "', updated_at=NOW(), status='Submitted'";
    if(count($update_parts) > 0) {
        $insert_sql .= ", " . implode(", ", $update_parts);
    }
    
    try {
        if ($con->query($insert_sql)) {
            // Delete old dynamic answers for this town to avoid duplicates
            $con->query("DELETE FROM survey_answers WHERE town_id=$target_town_id");
            
            // Insert into Dynamic Table
            foreach($dyn_answers as $q_id => $val) {
                if(is_array($val)) $val = implode(", ", $val);
                $clean_val = mysqli_real_escape_string($con, $val);
                $con->query("INSERT INTO survey_answers (town_id, question_id, answer_text) VALUES ($target_town_id, $q_id, '$clean_val')");
            }
            
            $msg = "ULB Profile saved successfully! Form has been reset.";
            $form_submitted_success = true;
        } else {
            $error = "Error saving profile data: " . $con->error;
        }
    } catch (Exception $e) {
        $error = "Database Error: " . $e->getMessage();
    }
  }
}
?>
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Know Your ULB - User Profile || PMIDC</title>
  <link rel="icon" type="image/png" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    .form-section-title { font-size: 1.15rem; font-weight: 700; color: #0056b3; border-bottom: 2px solid #e9ecef; padding-bottom: 8px; margin-bottom: 20px; }
    .nav-tabs .nav-link { font-size: 0.85rem; font-weight: 600; padding: 10px 14px; color: #495057; border-radius: 4px 4px 0 0; }
    .nav-tabs .nav-link.active { background-color: #007bff; color: #ffffff; border-color: #007bff; }
    .card-form { border: 1px solid #e3e6f0; border-radius: 8px; box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15); }
    .badge-qno { background-color: #e3f2fd; color: #0d47a1; font-weight: 700; padding: 4px 8px; border-radius: 4px; margin-right: 6px; }
    .q-label { font-weight: 600; color: #2c3e50; }
    .req-star { color: #dc3545; font-weight: bold; margin-left: 2px; }
    .tab-footer-nav { background-color: #f8f9fa; border-top: 1px solid #e9ecef; padding: 15px; margin-top: 25px; border-radius: 0 0 6px 6px; }
  </style>
</head>
<body>
  <div class="container-scroller">
    <?php include_once('includes/header.php'); ?>
    <div class="container-fluid page-body-wrapper">
      <?php include_once('includes/sidebar.php'); ?>
      <div class="main-panel">
        <div class="content-wrapper">
          <?php if (!empty($msg)) echo "<div class='alert alert-success'>$msg</div>"; ?>
          <?php if (!empty($error)) echo "<div class='alert alert-danger'>$error</div>"; ?>

          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card card-form">
                <div class="card-body">
                  <h4 class="card-title text-primary"><i class="typcn typcn-clipboard"></i> Know Your ULB — Dynamic Profile</h4>
                  <p class="card-description">Comprehensive survey for <strong><?php echo htmlspecialchars($ulb_name); ?></strong>. Fields marked with <span class="req-star">*</span> are mandatory.</p>
                  
                  <form method="post" id="ulbProfileForm">
                    <?php if ($user_town_id == 0): ?>
                      <div class="form-group row bg-light p-3 rounded">
                        <label class="col-sm-3 col-form-label font-weight-bold">Select ULB / Town:</label>
                        <div class="col-sm-6">
                          <select name="selected_town_id" class="form-control">
                            <option value="">-- Select ULB Town --</option>
                            <?php
                            $sql_t = "SELECT town_id, town_name, district_name FROM towns_streetlight_mapping ORDER BY district_name, town_name";
                            $res_t = $con->query($sql_t);
                            while ($r_t = $res_t->fetch_assoc()) {
                              echo "<option value='" . $r_t['town_id'] . "'>" . htmlspecialchars($r_t['district_name'] . " - " . $r_t['town_name']) . "</option>";
                            }
                            ?>
                          </select>
                        </div>
                      </div>
                    <?php endif; ?>

                    <ul class="nav nav-tabs nav-fill mb-4" id="profileTab" role="tablist">
                      <?php
                      $dyn_cats = mysqli_query($con, "SELECT * FROM survey_categories ORDER BY display_order ASC, id ASC");
                      $first = true;
                      while($cat = mysqli_fetch_assoc($dyn_cats)) {
                          $active_cls = $first ? 'active' : '';
                          echo '<li class="nav-item"><a class="nav-link '.$active_cls.'" data-toggle="tab" href="#cat-'.$cat['id'].'" role="tab">'.htmlspecialchars($cat['category_name']).'</a></li>';
                          $first = false;
                      }
                      ?>
                    </ul>

                    <div class="tab-content" id="profileTabContent">
                      <?php
                      mysqli_data_seek($dyn_cats, 0);
                      $first = true;
                      $global_q = 1;
                      while($cat = mysqli_fetch_assoc($dyn_cats)) {
                          $active_cls = $first ? 'show active' : '';
                          echo '<div class="tab-pane fade '.$active_cls.' p-3" id="cat-'.$cat['id'].'" role="tabpanel">';
                          echo '<div class="form-section-title">'.htmlspecialchars($cat['category_name']).'</div>';
                          echo '<div class="row">';
                          
                          $q_query = mysqli_query($con, "SELECT * FROM survey_questions WHERE category_id = ".$cat['id']." ORDER BY display_order ASC, id ASC");
                          if(mysqli_num_rows($q_query) > 0) {
                              while($q = mysqli_fetch_assoc($q_query)) {
                                  $req = $q['is_required'] ? '<span class="req-star">*</span>' : '';
                                  $req_attr = $q['is_required'] ? 'required' : '';
                                  $input_name = "ans[".$q['id']."]";
                                  
                                  // Special pre-fills for basic info
                                  $val = '';
                                  $readonly = '';
                                  if($q['mapped_column'] == 'ulb_name') { $val = $ulb_name; $readonly = 'readonly'; }
                                  if($q['mapped_column'] == 'district_name') { $val = $district_name; $readonly = 'readonly'; }
                                  
                                  // Full width for checkboxes/textareas/grids if needed
                                  $col_class = ($q['input_type'] == 'checkbox' || $q['input_type'] == 'revenue_grid') ? 'col-md-12' : 'col-md-6';
                                  echo '<div class="'.$col_class.' form-group">';
                                  echo '<label class="q-label"><span class="badge-qno">Q'.$global_q.'</span> '.htmlspecialchars($q['question_text']).$req.'</label>';
                                  
                                  if($q['input_type'] == 'textarea') {
                                      echo '<textarea class="form-control" name="'.$input_name.'" '.$req_attr.' rows="2">'.$val.'</textarea>';
                                  } else if($q['input_type'] == 'number') {
                                      echo '<input type="number" step="0.01" class="form-control" name="'.$input_name.'" '.$req_attr.' value="'.$val.'" '.$readonly.'>';
                                      } else if($q['input_type'] == 'select') {
                                          $opts = explode(';', $q['options_text']); // The excel sheet uses semicolons or commas. We'll use comma below in our script, but explode on comma.
                                          // Actually, let's normalize to comma.
                                          $opts = explode(',', $q['options_text']);
                                          echo '<select class="form-control" name="'.$input_name.'" '.$req_attr.' '.($readonly? 'disabled':'').'>';
                                          echo '<option value="">-- Select --</option>';
                                          foreach($opts as $opt) {
                                              $opt = trim($opt);
                                              $sel = ($val == $opt) ? 'selected' : '';
                                              echo '<option value="'.htmlspecialchars($opt).'" '.$sel.'>'.htmlspecialchars($opt).'</option>';
                                          }
                                          echo '</select>';
                                          if($readonly) echo '<input type="hidden" name="'.$input_name.'" value="'.htmlspecialchars($val).'">';
                                      } else if($q['input_type'] == 'revenue_grid') {
                                          $rev_items = ["Water charges","Sewerage charges","Solid waste user charges","Trade licence fees","Building plan approval fees","Development charges","Fire NOC fees","Advertisement fees","Parking fees","Rent from ULB-owned properties","Municipal market fees","Slaughterhouse fees","Birth and death certificate fees","Road-cutting charges","Telecom and right-of-way charge","Tehbazari and street-vending fees","Community hall charges","Crematorium charges","Bus stand fees","Interest income","Penalties and compounding fees","Other own-source revenue"];
                                          echo '<div class="row px-3 mt-2">';
                                          // Decode existing values if any (for future edit mode)
                                          $existing_rev = json_decode($val, true);
                                          if(!is_array($existing_rev)) $existing_rev = [];
                                          
                                          foreach($rev_items as $item) {
                                              $safe_key = strtolower(str_replace([' ', '-'], '_', $item));
                                              $item_val = isset($existing_rev[$safe_key]) ? $existing_rev[$safe_key] : '';
                                              echo '<div class="col-md-6 mb-2 d-flex align-items-center">';
                                              echo '<label style="width:60%; margin-bottom:0; font-size:0.85rem;">'.htmlspecialchars($item).'</label>';
                                              echo '<input type="number" step="0.01" class="form-control form-control-sm" style="width:40%;" name="ans['.$q['id'].']['.$safe_key.']" value="'.htmlspecialchars($item_val).'" placeholder="₹ Lakh">';
                                              echo '</div>';
                                          }
                                          echo '</div>';
                                      } else if($q['input_type'] == 'checkbox') {
                                      $opts = explode(',', $q['options_text']);
                                      echo '<div class="row px-3 mt-2">';
                                      foreach($opts as $opt) {
                                          $opt = trim($opt);
                                          echo '<div class="col-md-4 mb-2">';
                                          echo '<label style="font-weight:500; display:flex; align-items:center; cursor:pointer;">';
                                          echo '<input type="checkbox" name="ans['.$q['id'].'][]" value="'.htmlspecialchars($opt).'" style="width:16px; height:16px; margin-right:8px; opacity:1 !important; position:relative !important; pointer-events:auto !important; appearance:auto !important; z-index:1;"> '.htmlspecialchars($opt);
                                          echo '</label></div>';
                                      }
                                      echo '</div>';
                                  } else {
                                      echo '<input type="text" class="form-control" name="'.$input_name.'" '.$req_attr.' value="'.$val.'" '.$readonly.'>';
                                  }
                                  echo '</div>';
                                  $global_q++;
                              }
                          } else {
                              echo '<div class="col-12"><p class="text-muted">No questions in this section.</p></div>';
                          }
                          
                          echo '</div></div>';
                          $first = false;
                      }
                      ?>
                    </div>

                    <div class="tab-footer-nav d-flex justify-content-between align-items-center">
                      <div>
                        <button type="button" class="btn btn-outline-secondary btn-prev font-weight-bold mr-2"><i class="typcn typcn-arrow-left-thick"></i> Prev</button>
                        <button type="button" class="btn btn-info btn-next font-weight-bold">Next <i class="typcn typcn-arrow-right-thick"></i></button>
                      </div>
                      <button type="submit" name="save_profile" class="btn btn-primary btn-lg font-weight-bold"><i class="typcn typcn-device-floppy"></i> Save Data</button>
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </div>
        </div>
        <?php include_once('includes/footer.php'); ?>
      </div>
    </div>
  </div>
  <script src="vendors/js/vendor.bundle.base.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  <script>
    $(document).ready(function () {
      <?php if ($form_submitted_success): ?>
        $('#ulbProfileForm')[0].reset();
        $('#profileTab a:first').tab('show');
      <?php endif; ?>
      $('.btn-next').click(function () {
        var $next = $('#profileTab .nav-link.active').parent().next().find('.nav-link');
        if ($next.length > 0) { $next.tab('show'); window.scrollTo(0,0); }
      });
      $('.btn-prev').click(function () {
        var $prev = $('#profileTab .nav-link.active').parent().prev().find('.nav-link');
        if ($prev.length > 0) { $prev.tab('show'); window.scrollTo(0,0); }
      });
    });
  </script>
</body>
</html>
