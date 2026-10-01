<?php
session_start();
error_reporting(0);
include('includes/config.php');

// Strict Admin Authorization Check (townid == 0)
$res_auth = $con->query("SELECT townid FROM streetlightlogin WHERE id=" . (int)$_SESSION['aid']);
if ($res_auth && $r_a = $res_auth->fetch_assoc()) {
  if ($r_a['townid'] != 0) {
    header('location:dashboard.php');
    exit();
  }
}

// Auto-create Dynamic Tables if not exists
$schema1 = "CREATE TABLE IF NOT EXISTS `ulb_categories` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `category_name` VARCHAR(255) NOT NULL,
  `category_code` VARCHAR(100) NOT NULL UNIQUE,
  `icon_class` VARCHAR(100) DEFAULT 'typcn-folder',
  `sort_order` INT DEFAULT 0,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
$con->query($schema1);

$schema2 = "CREATE TABLE IF NOT EXISTS `ulb_questions` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `category_id` INT NOT NULL,
  `question_code` VARCHAR(50) DEFAULT NULL,
  `question_text` TEXT NOT NULL,
  `input_type` VARCHAR(50) NOT NULL DEFAULT 'text',
  `choice_options` TEXT DEFAULT NULL,
  `is_mandatory` TINYINT(1) DEFAULT 0,
  `sort_order` INT DEFAULT 0,
  `is_active` TINYINT(1) DEFAULT 1,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
$con->query($schema2);

$schema3 = "CREATE TABLE IF NOT EXISTS `ulb_question_responses` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `submission_id` INT NOT NULL,
  `town_id` INT NOT NULL,
  `question_id` INT NOT NULL,
  `response_value` TEXT DEFAULT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
$con->query($schema3);

// Seed Categories if empty
$res_c_check = $con->query("SELECT COUNT(*) as cnt FROM ulb_categories");
if ($res_c_check && $res_c_check->fetch_assoc()['cnt'] == 0) {
  $seed = "INSERT IGNORE INTO `ulb_categories` (`id`, `category_name`, `category_code`, `icon_class`, `sort_order`) VALUES
  (1, 'Basic Profile', 'basic', 'typcn-user', 1),
  (2, 'Assets & Land', 'assets', 'typcn-home', 2),
  (3, 'Community Infrastructure', 'community', 'typcn-group', 3),
  (4, 'Digital Infrastructure', 'digital', 'typcn-camera', 4),
  (5, 'Energy & Streetlighting', 'energy', 'typcn-flash', 5),
  (6, 'Finance & Accounts', 'finance', 'typcn-calculator', 6),
  (7, 'Health & Education', 'health', 'typcn-heart', 7),
  (8, 'Horticulture & Parks', 'horticulture', 'typcn-tree', 8),
  (9, 'Institutional & Staffing', 'institutional', 'typcn-briefcase', 9),
  (10, 'Mobility & Transport', 'mobility', 'typcn-bus', 10),
  (11, 'Public Amenities', 'amenities', 'typcn-key', 11),
  (12, 'Urban Livelihood', 'livelihood', 'typcn-shopping-bag', 12),
  (13, 'WASH (Water & Sanitation)', 'wash', 'typcn-waves', 13);";
  $con->query($seed);
}

$msg = "";
$error = "";

// Handle Delete Question Action
if (isset($_GET['del_id']) && (int)$_GET['del_id'] > 0) {
  $del_id = (int)$_GET['del_id'];
  if ($con->query("DELETE FROM ulb_questions WHERE id = $del_id")) {
    $msg = "Question deleted successfully!";
  } else {
    $error = "Error deleting question: " . $con->error;
  }
}

// Handle Add / Edit Question Submission
if (isset($_POST['save_question'])) {
  $q_id = !empty($_POST['question_id']) ? (int)$_POST['question_id'] : 0;
  $category_id = (int)$_POST['category_id'];
  $question_code = mysqli_real_escape_string($con, $_POST['question_code']);
  $question_text = mysqli_real_escape_string($con, $_POST['question_text']);
  $input_type = mysqli_real_escape_string($con, $_POST['input_type']);
  $choice_options = mysqli_real_escape_string($con, $_POST['choice_options']);
  $is_mandatory = isset($_POST['is_mandatory']) ? 1 : 0;
  $sort_order = !empty($_POST['sort_order']) ? (int)$_POST['sort_order'] : 0;

  if ($category_id <= 0 || empty($question_text)) {
    $error = "Please select a Category and enter Question Text.";
  } else {
    if ($q_id > 0) {
      // Update
      $sql_q = "UPDATE ulb_questions SET 
        category_id = $category_id,
        question_code = '$question_code',
        question_text = '$question_text',
        input_type = '$input_type',
        choice_options = '$choice_options',
        is_mandatory = $is_mandatory,
        sort_order = $sort_order
        WHERE id = $q_id";
      if ($con->query($sql_q)) {
        $msg = "Question updated successfully!";
      } else {
        $error = "Error updating question: " . $con->error;
      }
    } else {
      // Insert New
      $sql_q = "INSERT INTO ulb_questions (category_id, question_code, question_text, input_type, choice_options, is_mandatory, sort_order) 
        VALUES ($category_id, '$question_code', '$question_text', '$input_type', '$choice_options', $is_mandatory, $sort_order)";
      if ($con->query($sql_q)) {
        $msg = "New question added successfully! It will now appear in the form and report.";
      } else {
        $error = "Error adding question: " . $con->error;
      }
    }
  }
}

// Load Question for Editing if Edit ID passed
$edit_q = array();
if (isset($_GET['edit_id']) && (int)$_GET['edit_id'] > 0) {
  $res_eq = $con->query("SELECT * FROM ulb_questions WHERE id = " . (int)$_GET['edit_id']);
  if ($res_eq && $res_eq->num_rows > 0) {
    $edit_q = $res_eq->fetch_assoc();
  }
}

// Filter handling for list
$filter_cat = isset($_GET['filter_cat']) ? (int)$_GET['filter_cat'] : 0;
$where_cat = ($filter_cat > 0) ? " WHERE q.category_id = $filter_cat " : "";

// Fetch Questions List
$sql_list = "SELECT q.*, c.category_name 
  FROM ulb_questions q 
  INNER JOIN ulb_categories c ON q.category_id = c.id 
  $where_cat 
  ORDER BY c.sort_order, q.sort_order, q.id DESC";
$res_list = $con->query($sql_list);
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <title>Dynamic Question Builder || Know Your ULB Admin</title>
  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    .card-builder {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
    }
    .table-questions th { background-color: #f8fafc; font-weight: 700; color: #1e293b; }
  </style>
</head>

<body>
  <div class="container-scroller">
    <?php include_once('includes/header.php'); ?>

    <nav class="navbar-breadcrumb col-xl-12 col-12 d-flex flex-row p-0">
      <div class="navbar-menu-wrapper d-flex align-items-center justify-content-end">
        <ul class="navbar-nav mr-lg-2">
          <li class="nav-item">
            <div class="d-flex align-items-baseline">
              <p class="mb-0">Home</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">Dynamic Question Builder</p>
            </div>
          </li>
        </ul>
      </div>
    </nav>

    <div class="container-fluid page-body-wrapper">
      <?php include_once('includes/sidebar.php'); ?>

      <div class="main-panel">
        <div class="content-wrapper">

          <?php if (!empty($msg)): ?>
            <div class="alert alert-success alert-dismissible fade show" role="alert">
              <strong>Success!</strong> <?php echo $msg; ?>
              <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                <span aria-hidden="true">&times;</span>
              </button>
            </div>
          <?php endif; ?>

          <?php if (!empty($error)): ?>
            <div class="alert alert-danger alert-dismissible fade show" role="alert">
              <strong>Error!</strong> <?php echo $error; ?>
              <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                <span aria-hidden="true">&times;</span>
              </button>
            </div>
          <?php endif; ?>

          <!-- Add / Edit Question Form Card -->
          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card card-builder">
                <div class="card-body">
                  <h4 class="card-title text-primary"><i class="typcn typcn-document-add"></i> <?php echo !empty($edit_q) ? "Edit Question" : "Add New Dynamic Question"; ?></h4>
                  <p class="card-description">
                    Add new questions to any survey category dynamically without modifying PHP code.
                  </p>

                  <form method="post" action="manage_questions.php">
                    <input type="hidden" name="question_id" value="<?php echo isset($edit_q['id']) ? $edit_q['id'] : ''; ?>">

                    <div class="row">
                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">Target Category <span class="text-danger">*</span></label>
                        <select name="category_id" class="form-control" required>
                          <option value="">-- Select Category --</option>
                          <?php
                          $res_cats = $con->query("SELECT * FROM ulb_categories ORDER BY sort_order");
                          while ($cat = $res_cats->fetch_assoc()) {
                            $sel = (isset($edit_q['category_id']) && $edit_q['category_id'] == $cat['id']) ? 'selected' : '';
                            echo "<option value='" . $cat['id'] . "' $sel>" . htmlspecialchars($cat['category_name']) . "</option>";
                          }
                          ?>
                        </select>
                      </div>

                      <div class="col-md-2 form-group">
                        <label class="font-weight-bold">Question Code</label>
                        <input type="text" name="question_code" class="form-control" placeholder="e.g. Q32B" value="<?php echo isset($edit_q['question_code']) ? htmlspecialchars($edit_q['question_code']) : ''; ?>">
                      </div>

                      <div class="col-md-6 form-group">
                        <label class="font-weight-bold">Question Text <span class="text-danger">*</span></label>
                        <input type="text" name="question_text" class="form-control" placeholder="Enter complete question title..." value="<?php echo isset($edit_q['question_text']) ? htmlspecialchars($edit_q['question_text']) : ''; ?>" required>
                      </div>

                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">Response Input Type <span class="text-danger">*</span></label>
                        <select name="input_type" class="form-control" required>
                          <option value="text" <?php if(isset($edit_q['input_type']) && $edit_q['input_type']=='text') echo 'selected'; ?>>Short Answer (Text)</option>
                          <option value="number" <?php if(isset($edit_q['input_type']) && $edit_q['input_type']=='number') echo 'selected'; ?>>Number (Integer / Count)</option>
                          <option value="decimal" <?php if(isset($edit_q['input_type']) && $edit_q['input_type']=='decimal') echo 'selected'; ?>>Decimal Amount (₹ Lakh / Acres / %)</option>
                          <option value="select_one" <?php if(isset($edit_q['input_type']) && $edit_q['input_type']=='select_one') echo 'selected'; ?>>Single Select (Dropdown / Yes-No)</option>
                          <option value="select_multiple" <?php if(isset($edit_q['input_type']) && $edit_q['input_type']=='select_multiple') echo 'selected'; ?>>Multiple Select (Checkboxes)</option>
                        </select>
                      </div>

                      <div class="col-md-5 form-group">
                        <label class="font-weight-bold">Choice Options (For Dropdowns / Checkboxes)</label>
                        <input type="text" name="choice_options" class="form-control" placeholder="e.g. Yes; No  or  Monthly; Quarterly; Yearly" value="<?php echo isset($edit_q['choice_options']) ? htmlspecialchars($edit_q['choice_options']) : ''; ?>">
                        <small class="text-muted">Separate multiple choices with a semicolon (<code>;</code>)</small>
                      </div>

                      <div class="col-md-3 form-group d-flex align-items-center mt-3">
                        <div class="form-check mr-3">
                          <label class="form-check-label font-weight-bold text-dark">
                            <input type="checkbox" name="is_mandatory" class="form-check-input" value="1" <?php if(isset($edit_q['is_mandatory']) && $edit_q['is_mandatory']==1) echo 'checked'; ?>>
                            Mandatory (*)
                          </label>
                        </div>
                      </div>
                    </div>

                    <div class="d-flex justify-content-between align-items-center mt-3">
                      <?php if (!empty($edit_q)): ?>
                        <a href="manage_questions.php" class="btn btn-outline-secondary btn-sm"><i class="typcn typcn-times"></i> Cancel Edit</a>
                      <?php else: ?>
                        <span></span>
                      <?php endif; ?>

                      <button type="submit" name="save_question" class="btn btn-primary font-weight-bold px-4">
                        <i class="typcn typcn-device-floppy"></i> <?php echo !empty($edit_q) ? "Update Question" : "Save New Question"; ?>
                      </button>
                    </div>

                  </form>
                </div>
              </div>
            </div>
          </div>

          <!-- Existing Questions List Table -->
          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card card-builder">
                <div class="card-body">
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <h4 class="card-title text-dark mb-0"><i class="typcn typcn-th-list"></i> Dynamic Questions Registry</h4>

                    <form method="get" class="form-inline">
                      <select name="filter_cat" class="form-control form-control-sm" onchange="this.form.submit()">
                        <option value="0">-- All Categories --</option>
                        <?php
                        $res_cats_f = $con->query("SELECT * FROM ulb_categories ORDER BY sort_order");
                        while ($catf = $res_cats_f->fetch_assoc()) {
                          $sel = ($filter_cat == $catf['id']) ? 'selected' : '';
                          echo "<option value='" . $catf['id'] . "' $sel>" . htmlspecialchars($catf['category_name']) . "</option>";
                        }
                        ?>
                      </select>
                    </form>
                  </div>

                  <div class="table-responsive">
                    <table class="table table-bordered table-striped table-hover table-questions">
                      <thead>
                        <tr>
                          <th>ID</th>
                          <th>Category</th>
                          <th>Code</th>
                          <th>Question Text</th>
                          <th>Input Type</th>
                          <th>Choices / Options</th>
                          <th>Mandatory</th>
                          <th>Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        <?php
                        if ($res_list && $res_list->num_rows > 0) {
                          while ($q_row = $res_list->fetch_assoc()) {
                        ?>
                            <tr>
                              <td>#<?php echo $q_row['id']; ?></td>
                              <td><span class="badge badge-info"><?php echo htmlspecialchars($q_row['category_name']); ?></span></td>
                              <td><strong><?php echo htmlspecialchars($q_row['question_code']); ?></strong></td>
                              <td><?php echo htmlspecialchars($q_row['question_text']); ?></td>
                              <td><span class="badge badge-secondary"><?php echo htmlspecialchars($q_row['input_type']); ?></span></td>
                              <td><small><?php echo htmlspecialchars($q_row['choice_options']); ?></small></td>
                              <td>
                                <?php if ($q_row['is_mandatory']): ?>
                                  <span class="badge badge-danger">Required *</span>
                                <?php else: ?>
                                  <span class="badge badge-light text-muted">Optional</span>
                                <?php endif; ?>
                              </td>
                              <td>
                                <a href="manage_questions.php?edit_id=<?php echo $q_row['id']; ?>" class="btn btn-warning btn-xs text-white"><i class="typcn typcn-edit"></i> Edit</a>
                                <a href="manage_questions.php?del_id=<?php echo $q_row['id']; ?>" onclick="return confirm('Are you sure you want to delete this question?')" class="btn btn-danger btn-xs"><i class="typcn typcn-trash"></i> Delete</a>
                              </td>
                            </tr>
                        <?php
                          }
                        } else {
                          echo "<tr><td colspan='8' class='text-center py-4 text-muted'>No custom dynamic questions added yet. Use the form above to add your first question!</td></tr>";
                        }
                        ?>
                      </tbody>
                    </table>
                  </div>
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
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
</body>

</html>
