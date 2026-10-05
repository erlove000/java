<?php
// Ensure $townid is available for the sidebar check
if (!isset($sidebar_townid)) {
    if (isset($_SESSION['aid'])) {
        $aid = (int)$_SESSION['aid'];
        $sidebar_query = mysqli_query($con, "SELECT townid FROM streetlightlogin WHERE id = $aid");
        if ($sidebar_query && mysqli_num_rows($sidebar_query) > 0) {
            $row = mysqli_fetch_assoc($sidebar_query);
            $sidebar_townid = $row['townid'];
        } else {
            $sidebar_townid = -1;
        }
    } else {
        $sidebar_townid = -1;
    }
}
?>
<div class="theme-setting-wrapper">
  <div id="settings-trigger"><i class="typcn typcn-cog-outline"></i></div>
  <div id="theme-settings" class="settings-panel">
    <i class="settings-close typcn typcn-times"></i>
    <p class="settings-heading">SIDEBAR SKINS</p>
    <div class="sidebar-bg-options selected" id="sidebar-light-theme">
      <div class="img-ss rounded-circle bg-light border mr-3"></div>Light
    </div>
    <div class="sidebar-bg-options" id="sidebar-dark-theme">
      <div class="img-ss rounded-circle bg-dark border mr-3"></div>Dark
    </div>
    <p class="settings-heading mt-2">HEADER SKINS</p>
    <div class="color-tiles mx-0 px-4">
      <div class="tiles success"></div>
      <div class="tiles warning"></div>
      <div class="tiles danger"></div>
      <div class="tiles info"></div>
      <div class="tiles dark"></div>
      <div class="tiles default"></div>
    </div>
  </div>
</div>

<nav class="sidebar sidebar-offcanvas" id="sidebar">
  <ul class="nav">
    <?php if ($sidebar_townid == '0'): ?>
    <li class="nav-item sidebar-category-header" style="margin-top: 5px !important;">
      <span class="nav-link" style="color: #0ea5e9 !important;">ADMIN CONTROLS</span>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="manage_users.php">
        <i class="typcn typcn-group-outline menu-icon text-info"></i>
        <span class="menu-title">Manage Users</span>
      </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="manage_survey.php">
        <i class="typcn typcn-edit menu-icon text-info"></i>
        <span class="menu-title">Manage Survey</span>
      </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="manage_towns.php">
        <i class="typcn typcn-location-outline menu-icon text-info"></i>
        <span class="menu-title">ULB / Town Master</span>
      </a>
    </li>
    <?php endif; ?>

    <li class="nav-item sidebar-category-header">
      <span class="nav-link">MAIN OPERATIONS</span>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="dashboard.php">
        <i class="typcn typcn-device-desktop menu-icon"></i>
        <span class="menu-title">Dashboard</span>
      </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="month_wisedata.php">
      <i class="typcn typcn-film menu-icon"></i>
      <span class="menu-title">Month Wise Data</span>
      </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="streetlight_details.php">
      <i class="typcn typcn-weather-sunny menu-icon"></i>
      <span class="menu-title">Add Streetlight Details</span>
      </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="add_reason.php">
      <i class="typcn typcn-message menu-icon"></i>
      <span class="menu-title">Add Reason</span>
      </a>
    </li>

    <li class="nav-item sidebar-category-header">
      <span class="nav-link">OPERATIONAL REPORTS</span>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="view-enquiry.php">
      <i class="typcn typcn-export menu-icon"></i> 
      <span class="menu-title">MIS Data Report</span>
      </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="search_details.php">
      <i class="typcn typcn-arrow-down-thick menu-icon"></i> 
      <span class="menu-title">MIS Reason Report</span>
      </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="PBTRAC.php">
      <i class="typcn typcn-clipboard menu-icon"></i> 
      <span class="menu-title">PBTRAC</span>
      </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="pbtrac_search.php">
      <i class="typcn typcn-folder menu-icon"></i> 
      <span class="menu-title">PBTRAC Report</span>
      </a>
    </li>

    <li class="nav-item sidebar-category-header">
      <span class="nav-link">KNOW YOUR ULB</span>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="user_profile.php">
      <i class="typcn typcn-document-text menu-icon"></i> 
      <span class="menu-title">Know Your ULB Survey</span>
      </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="user_profile_report.php">
      <i class="typcn typcn-chart-bar-outline menu-icon"></i> 
      <span class="menu-title">Know Your ULB Report</span>
      </a>
    </li>
  </ul>
</nav>