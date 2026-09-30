<?php 
session_start();
include_once('includes/config.php');

// Initialize variables
$from_date = "";
$to_date = "";

?>

<!DOCTYPE html>
<html lang="en">
<head>
    <title>Streetlight || Search Details</title>
    <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
    <link rel="stylesheet" href="vendors/typicons/typicons.css">
    <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
    <link rel="stylesheet" href="vendors/select2/select2.min.css">
    <link rel="stylesheet" href="vendors/select2-bootstrap-theme/select2-bootstrap.min.css">
    <link rel="stylesheet" href="css/vertical-layout-light/style.css">
</head>

<body>
    <div class="container-scroller">
        <nav class="navbar col-lg-12 col-12 p-0 fixed-top d-flex flex-row">
            <div class="navbar-brand-wrapper d-flex justify-content-center">
                <div class="navbar-brand-inner-wrapper d-flex justify-content-between align-items-center w-100">
                    <a class="navbar-brand brand-logo" href="index.html"><img src="images/logo.svg" alt="logo"/></a>
                    <a class="navbar-brand brand-logo-mini" href="index.html"><img src="images/logo-mini.svg" alt="logo"/></a>
                    <button class="navbar-toggler navbar-toggler align-self-center" type="button" data-toggle="minimize">
                        <span class="typcn typcn-th-menu"></span>
                    </button>
                </div>
            </div>
            <?php include_once('includes/header.php');?>
        </nav>

        <div class="container-fluid page-body-wrapper">
            <?php include_once('includes/sidebar.php');?> 

            <div class="main-panel">        
                <div class="content-wrapper">
                    <div class="row">
                        <div class="col-md-12 grid-margin stretch-card">
                            <div class="card">
                                <div class="card-body">
                                    <h4 class="card-title">Search Added Data Details</h4>
                                    <!-- Form sends data to mis_report.php -->
                                    <form class="forms-sample" method="POST" action="mis_report.php">  
                                        <div class="form-group">
                                            <label for="from_date">From Date</label>
                                            <input id="from_date" name="from_date" type="date" class="form-control" required="true">
                                        </div>
                                        
                                        <div class="form-group">
                                            <label for="to_date">To Date</label>
                                            <input id="to_date" name="to_date" type="date" class="form-control" required="true">
                                        </div>

                                        <button type="submit" class="btn btn-primary mr-2" name="search">Search</button>
                                    </form>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <?php include_once('includes/footer.php');?>
            </div>
        </div>
    </div>

    <!-- JavaScript -->
    <script src="vendors/js/vendor.bundle.base.js"></script>
</body>
</html>
