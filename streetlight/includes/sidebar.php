<?php
$current_page = basename($_SERVER['PHP_SELF']);
function get_active_class($page, $current) {
  return ($page == $current) ? 'active' : '';
}
?>

<nav class="sidebar sidebar-offcanvas" id="sidebar">
  <ul class="nav">
    <div class="sidebar-category-header">Main Operations</div>

    <li class="nav-item <?php echo get_active_class('dashboard.php', $current_page); ?>">
      <a class="nav-link" href="dashboard.php">
        <i class="typcn typcn-device-desktop menu-icon"></i>
        <span class="menu-title">Dashboard</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('month_wisedata.php', $current_page); ?>">
      <a class="nav-link" href="month_wisedata.php">
        <i class="typcn typcn-calendar-outline menu-icon"></i>
        <span class="menu-title">Month Wise Data</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('streetlight_details.php', $current_page); ?>">
      <a class="nav-link" href="streetlight_details.php">
        <i class="typcn typcn-weather-sunny menu-icon"></i>
        <span class="menu-title">Add Streetlight Details</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('add_reason.php', $current_page); ?>">
      <a class="nav-link" href="add_reason.php">
        <i class="typcn typcn-message-typing menu-icon"></i>
        <span class="menu-title">Add Reason</span>
      </a>
    </li>

    <div class="sidebar-category-header">Operational Reports</div>

    <li class="nav-item <?php echo get_active_class('view-enquiry.php', $current_page); ?>">
      <a class="nav-link" href="view-enquiry.php">
        <i class="typcn typcn-export-outline menu-icon"></i>
        <span class="menu-title">MIS Data Report</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('search_details.php', $current_page); ?>">
      <a class="nav-link" href="search_details.php">
        <i class="typcn typcn-document-text menu-icon"></i>
        <span class="menu-title">MIS Reason Report</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('PBTRAC.php', $current_page); ?>">
      <a class="nav-link" href="PBTRAC.php">
        <i class="typcn typcn-clipboard menu-icon"></i>
        <span class="menu-title">PBTRAC</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('pbtrac_search.php', $current_page); ?>">
      <a class="nav-link" href="pbtrac_search.php">
        <i class="typcn typcn-folder-open menu-icon"></i>
        <span class="menu-title">PBTRAC Report</span>
      </a>
    </li>

    <div class="sidebar-category-header">Know Your ULB</div>

    <li class="nav-item <?php echo get_active_class('user_profile.php', $current_page); ?>">
      <a class="nav-link" href="user_profile.php">
        <i class="typcn typcn-home-outline menu-icon"></i>
        <span class="menu-title">Know Your ULB Survey</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('user_profile_report.php', $current_page); ?>">
      <a class="nav-link" href="user_profile_report.php">
        <i class="typcn typcn-chart-bar-outline menu-icon"></i>
        <span class="menu-title">Know Your ULB Report</span>
      </a>
    </li>

    <?php
    // Check if logged in user is State Admin (townid = 0)
    $is_state_admin = false;
    if (isset($_SESSION['aid'])) {
      $sql_admin_chk = "SELECT townid FROM streetlightlogin WHERE id = " . (int)$_SESSION['aid'];
      $res_admin_chk = $con->query($sql_admin_chk);
      if ($res_admin_chk && $r_adm = $res_admin_chk->fetch_assoc()) {
        if ($r_adm['townid'] == 0) {
          $is_state_admin = true;
        }
      }
    }
    if ($is_state_admin):
    ?>
    <div class="sidebar-category-header text-primary font-weight-bold">Master Admin Control</div>

    <li class="nav-item <?php echo get_active_class('admin_dashboard.php', $current_page); ?>">
      <a class="nav-link" href="admin_dashboard.php">
        <i class="typcn typcn-cog-outline menu-icon"></i>
        <span class="menu-title">Admin Dashboard</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('manage_questions.php', $current_page); ?>">
      <a class="nav-link" href="manage_questions.php">
        <i class="typcn typcn-document-add menu-icon"></i>
        <span class="menu-title">Manage Questions</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('manage_users.php', $current_page); ?>">
      <a class="nav-link" href="manage_users.php">
        <i class="typcn typcn-group-outline menu-icon"></i>
        <span class="menu-title">Manage Users</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('manage_ulbs.php', $current_page); ?>">
      <a class="nav-link" href="manage_ulbs.php">
        <i class="typcn typcn-location-outline menu-icon"></i>
        <span class="menu-title">Manage ULBs & Targets</span>
      </a>
    </li>

    <li class="nav-item <?php echo get_active_class('manage_streetlight_data.php', $current_page); ?>">
      <a class="nav-link" href="manage_streetlight_data.php">
        <i class="typcn typcn-flash-outline menu-icon"></i>
        <span class="menu-title">Audit Operational Logs</span>
      </a>
    </li>
    <?php endif; ?>
  </ul>
</nav>
