<?php
session_start();
include('includes/config.php');

// Validate Session & Admin Access
if(strlen($_SESSION['aid'])==0) { 
  header('location:index.php');
  exit();
}

$aid = (int)$_SESSION['aid'];
$admin_check = mysqli_query($con, "SELECT townid FROM streetlightlogin WHERE id = $aid");
$admin_data = mysqli_fetch_assoc($admin_check);
if($admin_data['townid'] != '0') {
    header('location:dashboard.php');
    exit();
}

// ---------------------------------------------------------
// AUTO-CREATE REQUIRED TABLES FOR DYNAMIC SURVEY (IF NOT EXIST)
// ---------------------------------------------------------
$create_categories_table = "
CREATE TABLE IF NOT EXISTS `survey_categories` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `category_name` varchar(255) NOT NULL,
  `display_order` int(11) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
";
mysqli_query($con, $create_categories_table);

$create_questions_table = "
CREATE TABLE IF NOT EXISTS `survey_questions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `category_id` int(11) NOT NULL,
  `question_text` text NOT NULL,
  `input_type` varchar(50) NOT NULL DEFAULT 'text',
  `is_required` tinyint(1) NOT NULL DEFAULT '0',
  `display_order` int(11) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
";
mysqli_query($con, $create_questions_table);

$create_answers_table = "
CREATE TABLE IF NOT EXISTS `survey_answers` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `town_id` int(11) NOT NULL,
  `question_id` int(11) NOT NULL,
  `answer_text` text,
  `submitted_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
";
mysqli_query($con, $create_answers_table);

// Pre-populate 13 Default Categories if empty
$check_cats = mysqli_query($con, "SELECT COUNT(*) as count FROM survey_categories");
$cat_count = mysqli_fetch_assoc($check_cats)['count'];

if($cat_count == 0) {
    $default_categories = [
        "1. Basic Profile", "2. Assets & Land", "3. Community Infra", 
        "4. Digital Infra", "5. Energy", "6. Finance & Accounts", 
        "7. Health & Edu", "8. Horticulture", "9. Institutional", 
        "10. Mobility", "11. Public Amenities", "12. Livelihood", "13. WASH"
    ];
    $order = 1;
    foreach($default_categories as $dc) {
        $clean_name = mysqli_real_escape_string($con, preg_replace('/^\d+\.\s*/', '', $dc));
        mysqli_query($con, "INSERT INTO survey_categories (category_name, display_order) VALUES ('$clean_name', $order)");
        $order++;
    }
}


// ---------------------------------------------------------
// HANDLE FORM SUBMISSIONS
// ---------------------------------------------------------

// Add New Category
if(isset($_POST['add_category'])) {
    $category_name = mysqli_real_escape_string($con, $_POST['category_name']);
    $display_order = (int)$_POST['display_order'];
    $insert = mysqli_query($con, "INSERT INTO survey_categories (category_name, display_order) VALUES ('$category_name', $display_order)");
    if($insert) { $msg = "<div class='alert alert-success'>Category added successfully!</div>"; }
    else { $msg = "<div class='alert alert-danger'>Failed to add category.</div>"; }
}

// Add New Question
if(isset($_POST['add_question'])) {
    $category_id = (int)$_POST['category_id'];
    $question_text = mysqli_real_escape_string($con, $_POST['question_text']);
    $input_type = mysqli_real_escape_string($con, $_POST['input_type']);
    $options_text = isset($_POST['options_text']) ? mysqli_real_escape_string($con, $_POST['options_text']) : NULL;
    $is_required = isset($_POST['is_required']) ? 1 : 0;
    
    // Auto-calculate display order
    $o_res = mysqli_query($con, "SELECT MAX(display_order) as mo FROM survey_questions WHERE category_id=$category_id");
    $max_order = mysqli_fetch_assoc($o_res)['mo'] + 1;

    $insert = mysqli_query($con, "INSERT INTO survey_questions (category_id, question_text, input_type, options_text, is_required, display_order) VALUES ($category_id, '$question_text', '$input_type', '$options_text', $is_required, $max_order)");
    if($insert) { $msg = "<div class='alert alert-success'>Question added successfully!</div>"; }
    else { $msg = "<div class='alert alert-danger'>Failed to add question.</div>"; }
}

// Edit Question
if(isset($_POST['edit_question'])) {
    $q_id = (int)$_POST['question_id'];
    $question_text = mysqli_real_escape_string($con, $_POST['question_text']);
    $input_type = mysqli_real_escape_string($con, $_POST['input_type']);
    $options_text = isset($_POST['options_text']) ? mysqli_real_escape_string($con, $_POST['options_text']) : NULL;
    $is_required = isset($_POST['is_required']) ? 1 : 0;
    
    $update = mysqli_query($con, "UPDATE survey_questions SET question_text='$question_text', input_type='$input_type', options_text='$options_text', is_required=$is_required WHERE id=$q_id");
    if($update) { $msg = "<div class='alert alert-success'>Question updated successfully!</div>"; }
    else { $msg = "<div class='alert alert-danger'>Failed to update question.</div>"; }
}

// Delete Question
if(isset($_POST['delete_question'])) {
    $q_id = (int)$_POST['question_id'];
    $delete = mysqli_query($con, "DELETE FROM survey_questions WHERE id=$q_id");
    if($delete) { $msg = "<div class='alert alert-success'>Question deleted successfully!</div>"; }
}

// Delete Category
if(isset($_POST['delete_category'])) {
    $c_id = (int)$_POST['category_id'];
    // Delete associated questions
    mysqli_query($con, "DELETE FROM survey_questions WHERE category_id=$c_id");
    // Delete the category
    $delete = mysqli_query($con, "DELETE FROM survey_categories WHERE id=$c_id");
    if($delete) { $msg = "<div class='alert alert-success'>Category and its questions deleted successfully!</div>"; }
    else { $msg = "<div class='alert alert-danger'>Failed to delete category.</div>"; }
}

// Edit Category
if(isset($_POST['edit_category'])) {
    $c_id = (int)$_POST['category_id'];
    $category_name = mysqli_real_escape_string($con, $_POST['category_name']);
    $display_order = (int)$_POST['display_order'];
    $update = mysqli_query($con, "UPDATE survey_categories SET category_name='$category_name', display_order=$display_order WHERE id=$c_id");
    if($update) { $msg = "<div class='alert alert-success'>Category updated successfully!</div>"; }
    else { $msg = "<div class='alert alert-danger'>Failed to update category.</div>"; }
}

?>
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Manage Survey Engine || Street Light Monitoring</title>
  <link rel="icon" type="image/png" href="images/pmidc.jpg" />
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  
  <style>
      .survey-card { background: #fff; border-radius: 12px; border: 1px solid #e2e8f0; box-shadow: 0 4px 15px rgba(0,0,0,0.03); padding: 25px; margin-bottom:20px; }
      .category-header { background: #f8fafc; padding: 12px 20px; border-radius: 8px; font-weight: 700; color: #1e293b; border-left: 4px solid #0284c7; margin-bottom: 15px; display:flex; justify-content:space-between; align-items:center; }
      .question-item { padding: 12px 20px; border-bottom: 1px solid #f1f5f9; display:flex; justify-content:space-between; align-items:center; }
      .question-item:last-child { border-bottom: none; }
      .q-type-badge { background: #e0f2fe; color: #0284c7; font-size: 0.75rem; padding: 3px 8px; border-radius: 4px; font-weight: 600; margin-left: 10px; }
      .q-req-badge { background: #fee2e2; color: #ef4444; font-size: 0.7rem; padding: 2px 6px; border-radius: 4px; font-weight: 600; margin-left: 5px; }
  </style>
</head>
<body>
  <div class="container-scroller">
    <?php include_once('includes/header.php');?>
    
    <nav class="navbar-breadcrumb col-xl-12 col-12 d-flex flex-row p-0">
      <div class="navbar-menu-wrapper d-flex align-items-center justify-content-end" align="right">
        <ul class="navbar-nav mr-lg-2">
          <li class="nav-item">
            <div class="d-flex align-items-baseline">
              <p class="mb-0">Home</p> <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">Admin Controls</p> <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">Manage Survey Engine</p>
            </div>
          </li>
        </ul>
      </div>
    </nav>
    
    <div class="container-fluid page-body-wrapper">
      <?php include_once('includes/sidebar.php');?>
      
      <div class="main-panel">
        <div class="content-wrapper">
          
          <div class="d-flex align-items-center justify-content-between mb-4">
            <h4 class="font-weight-bold text-dark mb-0">Dynamic Survey Engine</h4>
            <div>
                <button class="btn btn-outline-primary bg-white" data-bs-toggle="modal" data-bs-target="#addCategoryModal">
                    <i class="fa-solid fa-folder-plus mr-2"></i> Add Section
                </button>
                <button class="btn btn-primary ml-2" style="background:#0284c7; border:none;" data-bs-toggle="modal" data-bs-target="#addQuestionModal">
                    <i class="fa-solid fa-plus mr-2"></i> Add Question
                </button>
            </div>
          </div>

          <?php if(isset($msg)) echo $msg; ?>

          <div class="row">
              <div class="col-12">
                  <div class="survey-card">
                      <h5 class="mb-4">Current Survey Structure (Know Your ULB)</h5>
                      
                      <?php
                      $cat_query = mysqli_query($con, "SELECT * FROM survey_categories ORDER BY display_order ASC, id ASC");
                      if(mysqli_num_rows($cat_query) > 0) {
                          while($category = mysqli_fetch_assoc($cat_query)) {
                              $cat_id = $category['id'];
                      ?>
                      
                      <div class="category-block mb-4">
                          <div class="category-header">
                              <span><i class="fa-regular fa-folder-open text-primary mr-2"></i> <?php echo htmlspecialchars($category['category_name']); ?></span>
                              <div>
                                  <button class="btn btn-sm text-primary" data-bs-toggle="modal" data-bs-target="#editCatModal<?php echo $category['id']; ?>"><i class="fa-solid fa-pen"></i></button>
                                  <button class="btn btn-sm text-danger" data-bs-toggle="modal" data-bs-target="#delCatModal<?php echo $category['id']; ?>"><i class="fa-solid fa-trash"></i></button>
                              </div>
                          </div>

                          <!-- Edit Category Modal -->
                          <div class="modal fade" id="editCatModal<?php echo $category['id']; ?>" tabindex="-1" aria-hidden="true">
                            <div class="modal-dialog">
                              <div class="modal-content" style="border-radius:12px; border:none;">
                                <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0;">
                                  <h5 class="modal-title font-weight-bold">Edit Section</h5>
                                  <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                                </div>
                                <form method="post">
                                    <div class="modal-body p-4">
                                        <input type="hidden" name="category_id" value="<?php echo $category['id']; ?>">
                                        <div class="mb-3">
                                            <label class="form-label font-weight-bold" style="font-size:0.85rem;">Section Name</label>
                                            <input type="text" name="category_name" class="form-control p-2" required value="<?php echo htmlspecialchars($category['category_name']); ?>">
                                        </div>
                                        <div class="mb-3">
                                            <label class="form-label font-weight-bold" style="font-size:0.85rem;">Display Order</label>
                                            <input type="number" name="display_order" class="form-control p-2" value="<?php echo (int)$category['display_order']; ?>">
                                        </div>
                                    </div>
                                    <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
                                        <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
                                        <button type="submit" name="edit_category" class="btn btn-primary" style="background:#0284c7;">Update Section</button>
                                    </div>
                                </form>
                              </div>
                            </div>
                          </div>

                          <!-- Delete Category Modal -->
                          <div class="modal fade" id="delCatModal<?php echo $category['id']; ?>" tabindex="-1" aria-hidden="true">
                            <div class="modal-dialog">
                              <div class="modal-content" style="border-radius:12px; border:none;">
                                <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0;">
                                  <h5 class="modal-title font-weight-bold text-danger">Delete Section</h5>
                                  <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                                </div>
                                <form method="post">
                                    <div class="modal-body p-4 text-center">
                                        <input type="hidden" name="category_id" value="<?php echo $category['id']; ?>">
                                        <i class="fa-solid fa-triangle-exclamation text-danger fa-3x mb-3"></i>
                                        <p>Are you sure you want to delete this section AND all its questions? This action cannot be undone.</p>
                                        <p class="text-muted small">"<?php echo htmlspecialchars($category['category_name']); ?>"</p>
                                    </div>
                                    <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
                                        <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
                                        <button type="submit" name="delete_category" class="btn btn-danger">Delete Permanently</button>
                                    </div>
                                </form>
                              </div>
                            </div>
                          </div>
                          <div class="question-list pl-4">
                              <?php
                              $q_query = mysqli_query($con, "SELECT * FROM survey_questions WHERE category_id = $cat_id ORDER BY display_order ASC, id ASC");
                              if(mysqli_num_rows($q_query) > 0) {
                                  while($q = mysqli_fetch_assoc($q_query)) {
                              ?>
                                  <div class="question-item">
                                      <div>
                                          <i class="fa-solid fa-caret-right text-muted mr-2"></i> 
                                          <span class="text-dark font-weight-500"><?php echo htmlspecialchars($q['question_text']); ?></span>
                                          <span class="q-type-badge"><?php echo strtoupper($q['input_type']); ?></span>
                                          <?php if($q['is_required']) echo "<span class='q-req-badge'>*REQ</span>"; ?>
                                      </div>
                                      <div>
                                          <button class="btn btn-sm text-primary" data-bs-toggle="modal" data-bs-target="#editQModal<?php echo $q['id']; ?>"><i class="fa-solid fa-pen"></i></button>
                                          <button class="btn btn-sm text-danger" data-bs-toggle="modal" data-bs-target="#delQModal<?php echo $q['id']; ?>"><i class="fa-solid fa-trash"></i></button>
                                      </div>
                                  </div>

                                  <!-- Edit Question Modal -->
                                  <div class="modal fade" id="editQModal<?php echo $q['id']; ?>" tabindex="-1" aria-hidden="true">
                                    <div class="modal-dialog">
                                      <div class="modal-content" style="border-radius:12px; border:none;">
                                        <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0;">
                                          <h5 class="modal-title font-weight-bold">Edit Question</h5>
                                          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                                        </div>
                                        <form method="post">
                                            <div class="modal-body p-4">
                                                <input type="hidden" name="question_id" value="<?php echo $q['id']; ?>">
                                                <div class="mb-3">
                                                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Question Text</label>
                                                    <textarea name="question_text" class="form-control p-2" required><?php echo htmlspecialchars($q['question_text']); ?></textarea>
                                                </div>
                                                <div class="mb-3">
                                                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Input Type</label>
                                                    <select name="input_type" class="form-select p-2" onchange="document.getElementById('editOptsWrap<?php echo $q['id']; ?>').style.display = (this.value=='checkbox' || this.value=='select') ? 'block' : 'none';" required>
                                                        <option value="text" <?php if($q['input_type']=='text') echo 'selected'; ?>>Short Text</option>
                                                        <option value="number" <?php if($q['input_type']=='number') echo 'selected'; ?>>Number</option>
                                                        <option value="textarea" <?php if($q['input_type']=='textarea') echo 'selected'; ?>>Long Text / Paragraph</option>
                                                        <option value="date" <?php if($q['input_type']=='date') echo 'selected'; ?>>Date</option>
                                                        <option value="checkbox" <?php if($q['input_type']=='checkbox') echo 'selected'; ?>>Multiple Choice (Checkboxes)</option>
                                                        <option value="select" <?php if($q['input_type']=='select') echo 'selected'; ?>>Dropdown (Select One)</option>
                                                    </select>
                                                </div>
                                                <div class="mb-3" id="editOptsWrap<?php echo $q['id']; ?>" style="display: <?php echo ($q['input_type']=='checkbox' || $q['input_type']=='select') ? 'block' : 'none'; ?>;">
                                                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Options (Checkboxes or Dropdown)</label>
                                                    <textarea name="options_text" class="form-control p-2" placeholder="Comma separated. e.g: Option 1, Option 2, Option 3"><?php echo htmlspecialchars($q['options_text']); ?></textarea>
                                                </div>
                                                <div class="form-check mt-3">
                                                    <input class="form-check-input" type="checkbox" name="is_required" id="reqCheck<?php echo $q['id']; ?>" <?php if($q['is_required']) echo 'checked'; ?>>
                                                    <label class="form-check-label" for="reqCheck<?php echo $q['id']; ?>">This question is required</label>
                                                </div>
                                            </div>
                                            <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
                                                <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
                                                <button type="submit" name="edit_question" class="btn btn-primary" style="background:#0284c7;">Update Question</button>
                                            </div>
                                        </form>
                                      </div>
                                    </div>
                                  </div>

                                  <!-- Delete Question Modal -->
                                  <div class="modal fade" id="delQModal<?php echo $q['id']; ?>" tabindex="-1" aria-hidden="true">
                                    <div class="modal-dialog">
                                      <div class="modal-content" style="border-radius:12px; border:none;">
                                        <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0;">
                                          <h5 class="modal-title font-weight-bold text-danger">Delete Question</h5>
                                          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                                        </div>
                                        <form method="post">
                                            <div class="modal-body p-4 text-center">
                                                <input type="hidden" name="question_id" value="<?php echo $q['id']; ?>">
                                                <i class="fa-solid fa-triangle-exclamation text-danger fa-3x mb-3"></i>
                                                <p>Are you sure you want to delete this question? This action cannot be undone.</p>
                                                <p class="text-muted small">"<?php echo htmlspecialchars($q['question_text']); ?>"</p>
                                            </div>
                                            <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
                                                <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
                                                <button type="submit" name="delete_question" class="btn btn-danger">Delete Permanently</button>
                                            </div>
                                        </form>
                                      </div>
                                    </div>
                                  </div>
                              <?php 
                                  } 
                              } else { 
                                  echo "<div class='text-muted small pl-4 pb-2'>No questions added to this section yet.</div>"; 
                              } 
                              ?>
                          </div>
                      </div>
                      
                      <?php 
                          } 
                      } else { 
                          echo "<div class='alert alert-light text-center border'>Your survey engine is currently empty. Click 'Add Section' to begin building dynamic questions.</div>";
                      } 
                      ?>
                  </div>
              </div>
          </div>

        </div>
        <?php include_once('includes/footer.php');?>
      </div>
    </div>
  </div>

  <!-- Add Category Modal -->
  <div class="modal fade" id="addCategoryModal" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog">
      <div class="modal-content" style="border-radius:12px; border:none;">
        <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0;">
          <h5 class="modal-title font-weight-bold">Add Survey Section</h5>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>
        <form method="post">
            <div class="modal-body p-4">
                <div class="mb-3">
                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Section Name</label>
                    <input type="text" name="category_name" class="form-control p-2" required placeholder="e.g. Section 5: Energy & Streetlighting">
                </div>
                <div class="mb-3">
                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Display Order</label>
                    <input type="number" name="display_order" class="form-control p-2" value="0">
                </div>
            </div>
            <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
                <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
                <button type="submit" name="add_category" class="btn btn-primary" style="background:#0284c7;">Create Section</button>
            </div>
        </form>
      </div>
    </div>
  </div>

  <!-- Add Question Modal -->
  <div class="modal fade" id="addQuestionModal" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog">
      <div class="modal-content" style="border-radius:12px; border:none;">
        <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0;">
          <h5 class="modal-title font-weight-bold">Add Dynamic Question</h5>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>
        <form method="post">
            <div class="modal-body p-4">
                <div class="mb-3">
                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Select Section</label>
                    <select name="category_id" class="form-select p-2" required>
                        <option value="">-- Select Section --</option>
                        <?php
                        $cat_dd = mysqli_query($con, "SELECT id, category_name FROM survey_categories ORDER BY display_order ASC");
                        while($c = mysqli_fetch_assoc($cat_dd)) {
                            echo "<option value='".$c['id']."'>".$c['category_name']."</option>";
                        }
                        ?>
                    </select>
                </div>
                <div class="mb-3">
                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Question Text</label>
                    <textarea name="question_text" class="form-control p-2" required placeholder="e.g. How many operational streetlights do you have?"></textarea>
                </div>
                <div class="mb-3">
                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Input Type</label>
                    <select name="input_type" class="form-select p-2" onchange="document.getElementById('addOptsWrap').style.display = (this.value=='checkbox' || this.value=='select') ? 'block' : 'none';" required>
                        <option value="text">Short Text</option>
                        <option value="number">Number</option>
                        <option value="textarea">Long Text / Paragraph</option>
                        <option value="date">Date</option>
                        <option value="checkbox">Multiple Choice (Checkboxes)</option>
                        <option value="select">Dropdown (Select One)</option>
                    </select>
                </div>
                <div class="mb-3" id="addOptsWrap" style="display:none;">
                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Options (Checkboxes or Dropdown)</label>
                    <textarea name="options_text" class="form-control p-2" placeholder="Comma separated. e.g: Option 1, Option 2, Option 3"></textarea>
                    <small class="text-muted">Separate options with a comma.</small>
                </div>
                <div class="form-check mt-3">
                    <input class="form-check-input" type="checkbox" name="is_required" id="reqCheck" checked>
                    <label class="form-check-label" for="reqCheck">This question is required to be answered</label>
                </div>
            </div>
            <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
                <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
                <button type="submit" name="add_question" class="btn btn-primary" style="background:#0284c7;">Save Question</button>
            </div>
        </form>
      </div>
    </div>
  </div>

  <script src="vendors/js/vendor.bundle.base.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
</body>
</html>
