<?php
session_start();
include('includes/config.php');

// Validate Session
if(strlen($_SESSION['aid'])==0) { 
  header('location:index.php');
  exit();
}

$aid = (int)$_SESSION['aid'];
$admin_check = mysqli_query($con, "SELECT townid FROM streetlightlogin WHERE id = $aid");
$admin_data = mysqli_fetch_assoc($admin_check);

if($admin_data['townid'] != '0') {
    // Not an admin, redirect back
    header('location:dashboard.php');
    exit();
}

// Handle Add User Form Submission
if(isset($_POST['add_user'])) {
    $staff_name = mysqli_real_escape_string($con, $_POST['staff_name']);
    $username = mysqli_real_escape_string($con, $_POST['username']);
    $password = md5($_POST['password']); // Using existing md5 logic for consistency for now
    $townid = (int)$_POST['townid'];
    $districtid = (int)$_POST['districtid'];
    
    // Check if username exists
    $check = mysqli_query($con, "SELECT ID FROM streetlightlogin WHERE UserName = '$username'");
    if(mysqli_num_rows($check) > 0) {
        $msg = "<div class='alert alert-danger'>Username already exists. Please choose another.</div>";
    } else {
        $insert = mysqli_query($con, "INSERT INTO streetlightlogin (UserName, Password, staff_name, townid, districtid) VALUES ('$username', '$password', '$staff_name', '$townid', '$districtid')");
        if($insert) {
            $msg = "<div class='alert alert-success'>User added successfully!</div>";
        } else {
            $msg = "<div class='alert alert-danger'>Error adding user.</div>";
        }
    }
}

// Handle Update Password
if(isset($_POST['update_password'])) {
    $user_id = (int)$_POST['user_id'];
    $new_password = md5($_POST['new_password']);
    
    $update = mysqli_query($con, "UPDATE streetlightlogin SET Password = '$new_password' WHERE ID = $user_id");
    if($update) {
        $msg = "<div class='alert alert-success'>Password updated successfully!</div>";
    } else {
        $msg = "<div class='alert alert-danger'>Error updating password.</div>";
    }
}
?>
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Manage Users || Street Light Monitoring</title>
  <link rel="icon" type="image/png" href="images/pmidc.jpg" />
  <!-- Bootstrap 5 CDN for DataTables and Modals -->
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  
  <style>
      .table-card {
          background: #fff;
          border-radius: 12px;
          border: 1px solid #e2e8f0;
          box-shadow: 0 4px 15px rgba(0,0,0,0.03);
          padding: 25px;
      }
      .table th { background: #f8fafc; color: #475569; font-weight: 700; text-transform: uppercase; font-size: 0.8rem; }
      .table td { vertical-align: middle; font-size: 0.9rem; color: #1e293b; font-weight: 500; }
      .badge-admin { background: #0284c7; color: white; padding: 5px 10px; border-radius: 20px; font-size: 0.75rem; }
      .badge-ulb { background: #10b981; color: white; padding: 5px 10px; border-radius: 20px; font-size: 0.75rem; }
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
              <p class="mb-0">Home</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">Admin Controls</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">Manage Users</p>
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
            <h4 class="font-weight-bold text-dark mb-0">System User Management</h4>
            <button class="btn btn-primary" style="background:#0284c7; border:none; border-radius:8px; font-weight:600;" data-bs-toggle="modal" data-bs-target="#addUserModal">
                <i class="fa-solid fa-plus mr-2"></i> Add New User
            </button>
          </div>

          <?php if(isset($msg)) echo $msg; ?>

          <div class="table-card">
              <div class="table-responsive">
                  <table class="table table-hover">
                      <thead>
                          <tr>
                              <th>ID</th>
                              <th>Staff / ULB Name</th>
                              <th>Username</th>
                              <th>Role</th>
                              <th>Town ID</th>
                              <th>District ID</th>
                              <th class="text-center">Actions</th>
                          </tr>
                      </thead>
                      <tbody>
                          <?php
                          $query = mysqli_query($con, "SELECT * FROM streetlightlogin ORDER BY ID ASC");
                          if(mysqli_num_rows($query) > 0) {
                              while($row = mysqli_fetch_assoc($query)) {
                                  $role = ($row['townid'] == '0') ? "<span class='badge-admin'><i class='fa-solid fa-crown mr-1'></i> Admin</span>" : "<span class='badge-ulb'><i class='fa-solid fa-building mr-1'></i> ULB User</span>";
                          ?>
                          <tr>
                              <td>#<?php echo $row['ID']; ?></td>
                              <td><?php echo htmlspecialchars($row['staff_name']); ?></td>
                              <td><strong><?php echo htmlspecialchars($row['UserName']); ?></strong></td>
                              <td><?php echo $role; ?></td>
                              <td><?php echo $row['townid']; ?></td>
                              <td><?php echo $row['districtid']; ?></td>
                              <td class="text-center">
                                  <button class="btn btn-sm btn-outline-info" data-bs-toggle="modal" data-bs-target="#editPassModal<?php echo $row['ID']; ?>" title="Change Password" <?php if($row['townid']=='0' && $row['ID'] != $aid) echo "disabled"; ?>>
                                      <i class="fa-solid fa-key"></i>
                                  </button>
                              </td>
                          </tr>

                          <!-- Edit Password Modal for this User -->
                          <div class="modal fade" id="editPassModal<?php echo $row['ID']; ?>" tabindex="-1" aria-hidden="true">
                            <div class="modal-dialog">
                              <div class="modal-content" style="border-radius:12px; border:none;">
                                <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0; border-radius:12px 12px 0 0;">
                                  <h5 class="modal-title font-weight-bold">Change Password: <?php echo htmlspecialchars($row['UserName']); ?></h5>
                                  <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                                </div>
                                <form method="post">
                                    <div class="modal-body p-4">
                                        <input type="hidden" name="user_id" value="<?php echo $row['ID']; ?>">
                                        <div class="mb-3">
                                            <label class="form-label font-weight-bold" style="font-size:0.85rem;">New Password</label>
                                            <input type="password" name="new_password" class="form-control p-2" required placeholder="Enter new password">
                                        </div>
                                    </div>
                                    <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
                                        <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
                                        <button type="submit" name="update_password" class="btn btn-primary" style="background:#0284c7;">Update Password</button>
                                    </div>
                                </form>
                              </div>
                            </div>
                          </div>

                          <?php } } else { ?>
                              <tr><td colspan="7" class="text-center text-muted">No users found.</td></tr>
                          <?php } ?>
                      </tbody>
                  </table>
              </div>
          </div>

        </div>
        <!-- content-wrapper ends -->
        <?php include_once('includes/footer.php');?>
      </div>
      <!-- main-panel ends -->
    </div>
  </div>

  <!-- Add User Modal -->
  <div class="modal fade" id="addUserModal" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog">
      <div class="modal-content" style="border-radius:12px; border:none;">
        <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0; border-radius:12px 12px 0 0;">
          <h5 class="modal-title font-weight-bold">Create New User</h5>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>
        <form method="post">
            <div class="modal-body p-4">
                <div class="mb-3">
                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Staff / ULB Name</label>
                    <input type="text" name="staff_name" class="form-control p-2" required placeholder="e.g. Mohali Admin">
                </div>
                <div class="mb-3">
                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Username</label>
                    <input type="text" name="username" class="form-control p-2" required placeholder="Unique login username">
                </div>
                <div class="mb-3">
                    <label class="form-label font-weight-bold" style="font-size:0.85rem;">Password</label>
                    <input type="password" name="password" class="form-control p-2" required placeholder="Strong password">
                </div>
                <div class="row">
                    <div class="col-md-6 mb-3">
                        <label class="form-label font-weight-bold" style="font-size:0.85rem;">Town ID</label>
                        <input type="number" name="townid" class="form-control p-2" required placeholder="e.g. 101 (0 for Admin)">
                    </div>
                    <div class="col-md-6 mb-3">
                        <label class="form-label font-weight-bold" style="font-size:0.85rem;">District ID</label>
                        <input type="number" name="districtid" class="form-control p-2" required placeholder="e.g. 1">
                    </div>
                </div>
                <div class="alert alert-info py-2 mt-2" style="font-size:0.8rem;">
                    <i class="fa-solid fa-circle-info mr-1"></i> Setting <strong>Town ID = 0</strong> makes this user an Admin.
                </div>
            </div>
            <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
                <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
                <button type="submit" name="add_user" class="btn btn-primary" style="background:#0284c7;">Create User</button>
            </div>
        </form>
      </div>
    </div>
  </div>

  <script src="vendors/js/vendor.bundle.base.js"></script>
  <!-- Bootstrap 5 JS for Modals -->
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
</body>
</html>
