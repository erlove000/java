<link rel="stylesheet" href="css/portal_custom.css?v=1.3">

<nav class="navbar col-lg-12 col-12 p-0 fixed-top d-flex flex-row">
  <div class="navbar-brand-wrapper d-flex align-items-center justify-content-between px-3">
    <a class="navbar-brand brand-logo d-flex align-items-center text-decoration-none" href="dashboard.php">
      <img src="images/pmidc.jpg" alt="PMIDC" style="height: 32px; width: auto; border-radius: 6px; margin-right: 10px;">
      <span class="font-weight-bold text-white" style="font-size: 0.95rem; letter-spacing: 0.3px; white-space: nowrap;">Street Light Monitoring</span>
    </a>
  </div>

  <div class="navbar-menu-wrapper d-flex align-items-center justify-content-between">
    <div class="d-flex align-items-center">
      <!-- Sidebar Toggle Button (3 lines icon) -->
      <button class="navbar-toggler align-self-center text-white sidebar-toggle-icon-btn mr-3" type="button" data-toggle="minimize" id="desktopSidebarToggle" title="Toggle Sidebar">
        <span class="typcn typcn-th-menu"></span>
      </button>

      <?php
      $adid = (int)$_SESSION['aid'];
      $header_staff_name = "User Account";
      $header_town_name = "State Admin";
      if ($adid > 0) {
        $query_h = "SELECT l.staff_name, l.townid, t.town_name FROM streetlightlogin l LEFT JOIN towns_streetlight_mapping t ON l.townid = t.town_id WHERE l.ID='$adid'";
        $ret_h = mysqli_query($con, $query_h);
        if ($ret_h && $row_h = mysqli_fetch_array($ret_h)) {
          $header_staff_name = !empty($row_h['staff_name']) ? $row_h['staff_name'] : "User";
          $header_town_name = ($row_h['townid'] == 0) ? "State Admin" : $row_h['town_name'];
        }
      }
      ?>

      <!-- Town / ULB Name Pill Badge -->
      <div class="d-none d-sm-block">
        <span class="town-badge-header">
          <i class="typcn typcn-location"></i> <?php echo htmlspecialchars($header_town_name); ?>
        </span>
      </div>
    </div>

    <ul class="navbar-nav align-items-center">
      <!-- User Profile Dropdown -->
      <li class="nav-item nav-profile dropdown">
        <a class="nav-link dropdown-toggle d-flex align-items-center text-decoration-none" href="javascript:void(0);" data-toggle="dropdown" id="profileDropdown">
          <img src="images/pmidc.jpg" alt="profile"/>
          <span class="nav-profile-name"><?php echo htmlspecialchars($header_staff_name); ?></span>
        </a>
        <div class="dropdown-menu dropdown-menu-right navbar-dropdown shadow-lg border-0" aria-labelledby="profileDropdown" id="profileDropdownMenu">
          <a class="dropdown-item py-2" href="profile.php">
            <i class="typcn typcn-user-outline text-primary mr-2"></i>
            My Profile
          </a>
          <a class="dropdown-item py-2" href="change-password.php">
            <i class="typcn typcn-key-outline text-primary mr-2"></i>
            Change Password
          </a>
          <div class="dropdown-divider"></div>
          <a class="dropdown-item py-2 text-danger" href="logout.php">
            <i class="typcn typcn-power text-danger mr-2"></i>
            Logout
          </a>
        </div>
      </li>
    </ul>
    
    <button class="navbar-toggler navbar-toggler-right d-lg-none align-self-center text-white" type="button" data-toggle="offcanvas">
      <span class="typcn typcn-th-menu"></span>
    </button>
  </div>
</nav>

<script type="text/javascript">
$(document).ready(function() {
  // Sidebar Minimizer Toggle Handler (Desktop)
  $(document).on('click', '[data-toggle="minimize"], #desktopSidebarToggle', function(e) {
    e.preventDefault();
    $('body').toggleClass('sidebar-icon-only');
  });

  // Sidebar Offcanvas Toggle Handler (Mobile)
  $(document).on('click', '[data-toggle="offcanvas"]', function(e) {
    e.preventDefault();
    $('.sidebar-offcanvas').toggleClass('active');
  });

  // Profile Dropdown Toggle Handler
  $(document).on('click', '#profileDropdown', function(e) {
    e.preventDefault();
    e.stopPropagation();
    $('#profileDropdownMenu').toggleClass('show');
  });

  $(document).on('click', function(e) {
    if (!$(e.target).closest('.nav-profile').length) {
      $('#profileDropdownMenu').removeClass('show');
    }
  });

  setInterval(runningTime, 1000);
});

function runningTime() {
  $.ajax({
    url: 'timeScript.php',
    success: function(data) {
       $('#runningTime').html(data);
     },
  });
}
</script>