<?php
session_start();
error_reporting(0);
include('includes/config.php');

if (strlen($_SESSION['aid']) == 0) {
  header('location:index.php');
  exit();
}

// Strict Admin Authorization Check (townid == 0)
$res_auth = $con->query("SELECT townid FROM streetlightlogin WHERE id=" . (int)$_SESSION['aid']);
if ($res_auth && $r_a = $res_auth->fetch_assoc()) {
  if ($r_a['townid'] != 0) {
    header('location:dashboard.php');
    exit();
  }
}

$msg = "";
$error = "";

// Handle Quick Password Reset Modal Form Submit
if (isset($_POST['quick_reset_password'])) {
  $reset_user_id = (int)$_POST['reset_user_id'];
  $new_password = $_POST['new_password'];

  if ($reset_user_id > 0 && !empty($new_password)) {
    $pass_hash = md5($new_password);
    if ($con->query("UPDATE streetlightlogin SET Password = '$pass_hash' WHERE ID = $reset_user_id")) {
      $msg = "Password reset successfully for user ID #$reset_user_id!";
    } else {
      $error = "Failed to reset password: " . $con->error;
    }
  } else {
    $error = "Please enter a valid new password.";
  }
}

// Handle Delete User Action
if (isset($_GET['del_id']) && (int)$_GET['del_id'] > 0) {
  $del_id = (int)$_GET['del_id'];
  if ($del_id == $_SESSION['aid']) {
    $error = "You cannot delete your own logged-in admin account!";
  } else {
    if ($con->query("DELETE FROM streetlightlogin WHERE ID = $del_id")) {
      $msg = "User account deleted successfully!";
    } else {
      $error = "Error deleting user account: " . $con->error;
    }
  }
}

// Handle Add / Edit User Submission
if (isset($_POST['save_user'])) {
  $user_id = !empty($_POST['user_id']) ? (int)$_POST['user_id'] : 0;
  $username = mysqli_real_escape_string($con, trim($_POST['username']));
  $raw_password = $_POST['password'];
  $staff_name = mysqli_real_escape_string($con, trim($_POST['staff_name']));
  $email = mysqli_real_escape_string($con, trim($_POST['email']));
  $mobile_no = mysqli_real_escape_string($con, trim($_POST['mobile_no']));
  $townid = (int)$_POST['townid'];

  // Fetch districtid corresponding to selected townid
  $districtid = 0;
  if ($townid > 0) {
    $res_d = $con->query("SELECT distid FROM towns_streetlight_mapping WHERE town_id = $townid");
    if ($res_d && $rd = $res_d->fetch_assoc()) {
      $districtid = $rd['distid'];
    }
  }

  if (empty($username) || empty($staff_name)) {
    $error = "Please fill all required user details.";
  } else {
    if ($user_id > 0) {
      // Update User
      $pass_update_sql = "";
      if (!empty($raw_password)) {
        $pass_hash = md5($raw_password);
        $pass_update_sql = ", Password = '$pass_hash' ";
      }
      $sql_u = "UPDATE streetlightlogin SET 
        UserName = '$username',
        staff_name = '$staff_name',
        Email = '$email',
        mobile_no = '$mobile_no',
        districtid = $districtid,
        townid = $townid
        $pass_update_sql
        WHERE ID = $user_id";

      if ($con->query($sql_u)) {
        $msg = "User account updated successfully!";
      } else {
        $error = "Error updating user account: " . $con->error;
      }
    } else {
      // Insert New User
      if (empty($raw_password)) {
        $error = "Password is required for new user account.";
      } else {
        // Check username uniqueness
        $res_chk = $con->query("SELECT ID FROM streetlightlogin WHERE UserName = '$username'");
        if ($res_chk && $res_chk->num_rows > 0) {
          $error = "Username '$username' already exists. Please choose a different username.";
        } else {
          $pass_hash = md5($raw_password);
          $sql_u = "INSERT INTO streetlightlogin (UserName, Password, staff_name, Email, mobile_no, districtid, townid) 
            VALUES ('$username', '$pass_hash', '$staff_name', '$email', '$mobile_no', $districtid, $townid)";
          if ($con->query($sql_u)) {
            $msg = "New user account created successfully!";
          } else {
            $error = "Error creating user account: " . $con->error;
          }
        }
      }
    }
  }
}

// Load User for Editing if Edit ID passed
$edit_user = array();
if (isset($_GET['edit_id']) && (int)$_GET['edit_id'] > 0) {
  $res_eu = $con->query("SELECT * FROM streetlightlogin WHERE ID = " . (int)$_GET['edit_id']);
  if ($res_eu && $res_eu->num_rows > 0) {
    $edit_user = $res_eu->fetch_assoc();
  }
}

// Search and Filter logic
$search_query = isset($_GET['search']) ? mysqli_real_escape_string($con, trim($_GET['search'])) : '';
$filter_district = isset($_GET['filter_district']) ? mysqli_real_escape_string($con, trim($_GET['filter_district'])) : '';

$where_clauses = array();
if (!empty($search_query)) {
  $where_clauses[] = "(l.staff_name LIKE '%$search_query%' OR l.UserName LIKE '%$search_query%' OR t.town_name LIKE '%$search_query%' OR t.district_name LIKE '%$search_query%' OR l.Email LIKE '%$search_query%' OR l.mobile_no LIKE '%$search_query%')";
}
if (!empty($filter_district)) {
  $where_clauses[] = "t.district_name = '$filter_district'";
}

$where_sql = !empty($where_clauses) ? " WHERE " . implode(" AND ", $where_clauses) : "";

// Fetch Users List
$sql_users = "SELECT l.*, t.town_name, t.district_name 
  FROM streetlightlogin l 
  LEFT JOIN towns_streetlight_mapping t ON l.townid = t.town_id 
  $where_sql 
  ORDER BY l.ID DESC";
$res_users = $con->query($sql_users);
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <title>User Account Manager || PMIDC Admin</title>
  <link rel="icon" type="image/png" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    .card-user {
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.1);
    }
    .table-users th { background-color: #f1f5f9; font-weight: 700; color: #1e293b; }
    .search-box {
      background: #ffffff;
      border-radius: 10px;
      border: 1px solid #cbd5e1;
    }
    .search-box:focus-within {
      border-color: #0284c7;
      box-shadow: 0 0 0 3px rgba(2, 132, 199, 0.15);
    }
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
              <p class="mb-0">Master Admin Control</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">User Account Manager</p>
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
              <i class="typcn typcn-tick-outline mr-1"></i> <strong>Success!</strong> <?php echo $msg; ?>
              <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                <span aria-hidden="true">&times;</span>
              </button>
            </div>
          <?php endif; ?>

          <?php if (!empty($error)): ?>
            <div class="alert alert-danger alert-dismissible fade show" role="alert">
              <i class="typcn typcn-warning-outline mr-1"></i> <strong>Error!</strong> <?php echo $error; ?>
              <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                <span aria-hidden="true">&times;</span>
              </button>
            </div>
          <?php endif; ?>

          <!-- Add / Edit User Form Card -->
          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card card-user">
                <div class="card-body">
                  <h4 class="card-title text-primary"><i class="typcn typcn-user-add"></i> <?php echo !empty($edit_user) ? "Edit User Account #" . $edit_user['ID'] : "Create New User Account"; ?></h4>
                  <p class="card-description">
                    Manage ULB staff logins, reset passwords, and assign town scoping.
                  </p>

                  <form method="post" action="manage_users.php">
                    <input type="hidden" name="user_id" value="<?php echo isset($edit_user['ID']) ? $edit_user['ID'] : ''; ?>">

                    <div class="row">
                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">Staff Name <span class="text-danger">*</span></label>
                        <input type="text" name="staff_name" class="form-control" placeholder="e.g. Sh. Gurpreet Singh" value="<?php echo isset($edit_user['staff_name']) ? htmlspecialchars($edit_user['staff_name']) : ''; ?>" required>
                      </div>

                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">Username <span class="text-danger">*</span></label>
                        <input type="text" name="username" class="form-control" placeholder="Username for login" value="<?php echo isset($edit_user['UserName']) ? htmlspecialchars($edit_user['UserName']) : ''; ?>" required>
                      </div>

                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">Password <?php echo !empty($edit_user) ? '<small class="text-muted">(Leave blank to keep unchanged)</small>' : '<span class="text-danger">*</span>'; ?></label>
                        <div class="input-group">
                          <input type="password" id="formPasswordInput" name="password" class="form-control" placeholder="Enter password" <?php if(empty($edit_user)) echo 'required'; ?>>
                          <div class="input-group-append">
                            <button type="button" class="btn btn-outline-secondary" id="genFormPassBtn" title="Generate Random Password"><i class="typcn typcn-key"></i> Gen</button>
                          </div>
                        </div>
                      </div>

                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">Email Address</label>
                        <input type="email" name="email" class="form-control" placeholder="email@example.com" value="<?php echo isset($edit_user['Email']) ? htmlspecialchars($edit_user['Email']) : ''; ?>">
                      </div>

                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">Mobile Number</label>
                        <input type="text" name="mobile_no" class="form-control" placeholder="10-digit Mobile No" value="<?php echo isset($edit_user['mobile_no']) ? htmlspecialchars($edit_user['mobile_no']) : ''; ?>">
                      </div>

                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">Assigned ULB Town <span class="text-danger">*</span></label>
                        <select name="townid" class="form-control" required>
                          <option value="0">-- State Administrator (All ULBs Access) --</option>
                          <?php
                          $res_towns = $con->query("SELECT town_id, town_name, district_name FROM towns_streetlight_mapping ORDER BY district_name, town_name");
                          while ($t = $res_towns->fetch_assoc()) {
                            $sel = (isset($edit_user['townid']) && $edit_user['townid'] == $t['town_id']) ? 'selected' : '';
                            echo "<option value='" . $t['town_id'] . "' $sel>" . htmlspecialchars($t['district_name'] . " - " . $t['town_name']) . "</option>";
                          }
                          ?>
                        </select>
                      </div>
                    </div>

                    <div class="d-flex justify-content-between align-items-center mt-3">
                      <?php if (!empty($edit_user)): ?>
                        <a href="manage_users.php" class="btn btn-outline-secondary btn-sm"><i class="typcn typcn-times"></i> Cancel Edit</a>
                      <?php else: ?>
                        <span></span>
                      <?php endif; ?>

                      <button type="submit" name="save_user" class="btn btn-primary font-weight-bold px-4">
                        <i class="typcn typcn-device-floppy"></i> <?php echo !empty($edit_user) ? "Update User Account" : "Create User Account"; ?>
                      </button>
                    </div>

                  </form>
                </div>
              </div>
            </div>
          </div>

          <!-- Existing Users List Table & Search Controls Card -->
          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card card-user">
                <div class="card-body">
                  <div class="d-flex flex-column flex-md-row justify-content-between align-items-md-center mb-4">
                    <h4 class="card-title text-dark mb-3 mb-md-0"><i class="typcn typcn-group"></i> User Logins Registry & Access Control</h4>

                    <!-- Server + Client Search Bar -->
                    <form method="get" action="manage_users.php" class="form-inline">
                      <div class="input-group search-box mr-2 mb-2 mb-md-0">
                        <div class="input-group-prepend">
                          <span class="input-group-text bg-transparent border-0"><i class="typcn typcn-zoom"></i></span>
                        </div>
                        <input type="text" id="userLiveSearchInput" name="search" class="form-control border-0" placeholder="Search staff, username, ULB..." value="<?php echo htmlspecialchars($search_query); ?>" style="min-width: 240px;">
                      </div>

                      <select name="filter_district" class="form-control mr-2 mb-2 mb-md-0" onchange="this.form.submit()">
                        <option value="">All Districts</option>
                        <?php
                        $res_dist_list = $con->query("SELECT DISTINCT district_name FROM towns_streetlight_mapping ORDER BY district_name");
                        while ($rdl = $res_dist_list->fetch_assoc()) {
                          $sd = ($filter_district == $rdl['district_name']) ? 'selected' : '';
                          echo "<option value='" . htmlspecialchars($rdl['district_name']) . "' $sd>" . htmlspecialchars($rdl['district_name']) . "</option>";
                        }
                        ?>
                      </select>

                      <button type="submit" class="btn btn-info btn-sm mr-1"><i class="typcn typcn-filter"></i> Filter</button>
                      <?php if (!empty($search_query) || !empty($filter_district)): ?>
                        <a href="manage_users.php" class="btn btn-outline-secondary btn-sm"><i class="typcn typcn-refresh"></i> Reset</a>
                      <?php endif; ?>
                    </form>
                  </div>

                  <div class="table-responsive">
                    <table class="table table-bordered table-striped table-hover table-users">
                      <thead>
                        <tr>
                          <th>ID</th>
                          <th>Staff Name</th>
                          <th>Username</th>
                          <th>Role / Town Access</th>
                          <th>District</th>
                          <th>Mobile</th>
                          <th>Email</th>
                          <th style="width: 220px;">Actions & Password Control</th>
                        </tr>
                      </thead>
                      <tbody id="userTableBody">
                        <?php
                        if ($res_users && $res_users->num_rows > 0) {
                          while ($u_row = $res_users->fetch_assoc()) {
                        ?>
                            <tr>
                              <td>#<?php echo $u_row['ID']; ?></td>
                              <td><strong><?php echo htmlspecialchars($u_row['staff_name']); ?></strong></td>
                              <td><span class="badge badge-info px-2 py-1"><?php echo htmlspecialchars($u_row['UserName']); ?></span></td>
                              <td>
                                <?php if ($u_row['townid'] == 0): ?>
                                  <span class="badge badge-success px-2 py-1">State Administrator</span>
                                <?php else: ?>
                                  <?php echo htmlspecialchars($u_row['town_name']); ?>
                                <?php endif; ?>
                              </td>
                              <td><?php echo htmlspecialchars($u_row['district_name']); ?></td>
                              <td><?php echo htmlspecialchars($u_row['mobile_no']); ?></td>
                              <td><?php echo htmlspecialchars($u_row['Email']); ?></td>
                              <td>
                                <div class="btn-group" role="group">
                                  <a href="manage_users.php?edit_id=<?php echo $u_row['ID']; ?>" class="btn btn-warning btn-xs text-white" title="Edit details"><i class="typcn typcn-edit"></i> Edit</a>
                                  
                                  <button type="button" class="btn btn-dark btn-xs btn-open-reset-modal" data-id="<?php echo $u_row['ID']; ?>" data-name="<?php echo htmlspecialchars($u_row['staff_name']); ?>" data-username="<?php echo htmlspecialchars($u_row['UserName']); ?>" title="Reset Password Only">
                                    <i class="typcn typcn-key"></i> Pass
                                  </button>

                                  <?php if ($u_row['ID'] != $_SESSION['aid']): ?>
                                    <a href="manage_users.php?del_id=<?php echo $u_row['ID']; ?>" onclick="return confirm('Are you sure you want to delete account for <?php echo htmlspecialchars($u_row['UserName']); ?>?')" class="btn btn-danger btn-xs" title="Delete account"><i class="typcn typcn-trash"></i></a>
                                  <?php endif; ?>
                                </div>
                              </td>
                            </tr>
                        <?php
                          }
                        } else {
                          echo "<tr><td colspan='8' class='text-center py-4 text-muted'>No user accounts found matching your search criteria.</td></tr>";
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

  <!-- Quick Reset Password Modal -->
  <div class="modal fade" id="resetPasswordModal" tabindex="-1" role="dialog" aria-labelledby="resetModalLabel" aria-hidden="true">
    <div class="modal-dialog" role="document">
      <div class="modal-content">
        <form method="post" action="manage_users.php">
          <div class="modal-header bg-dark text-white">
            <h5 class="modal-title" id="resetModalLabel"><i class="typcn typcn-key"></i> Quick Reset User Password</h5>
            <button type="button" class="close text-white" data-dismiss="modal" aria-label="Close">
              <span aria-hidden="true">&times;</span>
            </button>
          </div>
          <div class="modal-body">
            <input type="hidden" name="reset_user_id" id="modal_reset_user_id">
            <div class="alert alert-info py-2 mb-3">
              Resetting password for: <strong id="modal_target_user_name"></strong> (<span id="modal_target_user_username"></span>)
            </div>

            <div class="form-group mb-3">
              <label class="font-weight-bold">New Password <span class="text-danger">*</span></label>
              <div class="input-group">
                <input type="text" name="new_password" id="modalNewPassword" class="form-control" placeholder="Enter new password" required>
                <div class="input-group-append">
                  <button type="button" class="btn btn-outline-secondary" id="btnGenModalPass"><i class="typcn typcn-key"></i> Auto-Generate</button>
                </div>
              </div>
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-secondary" data-dismiss="modal">Cancel</button>
            <button type="submit" name="quick_reset_password" class="btn btn-primary font-weight-bold"><i class="typcn typcn-key"></i> Save New Password</button>
          </div>
        </form>
      </div>
    </div>
  </div>

  <script src="vendors/js/vendor.bundle.base.js"></script>
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>

  <script>
    $(document).ready(function () {
      // Instant Client-Side Live Search Filter as you type
      $('#userLiveSearchInput').on('keyup', function () {
        var value = $(this).val().toLowerCase();
        $('#userTableBody tr').filter(function () {
          $(this).toggle($(this).text().toLowerCase().indexOf(value) > -1);
        });
      });

      // Quick Password Reset Modal trigger
      $('.btn-open-reset-modal').click(function () {
        var uid = $(this).data('id');
        var name = $(this).data('name');
        var uname = $(this).data('username');

        $('#modal_reset_user_id').val(uid);
        $('#modal_target_user_name').text(name);
        $('#modal_target_user_username').text(uname);
        $('#modalNewPassword').val('');
        $('#resetPasswordModal').modal('show');
      });

      // Helper function to generate random password
      function generatePass() {
        var chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@#$!";
        var pass = "";
        for (var i = 0; i < 8; i++) {
          pass += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        return pass;
      }

      $('#btnGenModalPass').click(function () {
        $('#modalNewPassword').val(generatePass());
      });

      $('#genFormPassBtn').click(function () {
        $('#formPasswordInput').val(generatePass()).attr('type', 'text');
      });
    });
  </script>
</body>

</html>
