<?php
session_start();
error_reporting(0);
include("includes/config.php");

// Redirect if already logged in
if (isset($_SESSION['aid']) && strlen($_SESSION['aid']) > 0) {
  header('location:dashboard.php');
  exit();
}

$error = "";

if (isset($_POST['submit'])) {
  $username = mysqli_real_escape_string($con, trim($_POST['username']));
  $password = md5($_POST['password']);

  $ret = mysqli_query($con, "SELECT ID, UserName, staff_name FROM streetlightlogin WHERE UserName='$username' AND Password='$password'");
  $num = mysqli_fetch_array($ret);
  if ($num > 0) {
    $_SESSION['alogin'] = $num['UserName'];
    $_SESSION['aid'] = $num['ID'];

    if (!empty($_POST["remember"])) {
      setcookie("user_login", $_POST["username"], time() + (10 * 365 * 24 * 60 * 60));
      setcookie("userpassword", $_POST["password"], time() + (10 * 365 * 24 * 60 * 60));
    } else {
      if (isset($_COOKIE["user_login"])) setcookie("user_login", "");
      if (isset($_COOKIE["userpassword"])) setcookie("userpassword", "");
    }

    header("location:dashboard.php");
    exit();
  } else {
    $error = "Invalid username or password. Please try again.";
  }
}
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
  <title>Login || PMIDC Street Light Monitoring System</title>
  <link rel="icon" type="image/png" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    html, body {
      height: 100%;
      margin: 0;
      font-family: 'Manrope', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background-color: #ffffff;
    }
    
    .login-container {
      min-height: 100vh;
      width: 100%;
      display: flex;
      margin: 0;
      padding: 0;
    }

    /* Left Hero Column - 58% Width */
    .hero-side {
      flex: 0 0 58%;
      max-width: 58%;
      background: linear-gradient(135deg, #0b1329 0%, #1e293b 45%, #0284c7 100%);
      color: #ffffff;
      padding: 60px 80px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      position: relative;
      overflow: hidden;
    }

    .hero-side::before {
      content: "";
      position: absolute;
      top: -100px;
      right: -100px;
      width: 450px;
      height: 450px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(2, 132, 199, 0.22) 0%, rgba(0,0,0,0) 70%);
      pointer-events: none;
    }

    .brand-header-logos {
      display: flex;
      align-items: center;
      gap: 15px;
    }

    .brand-logo-card {
      background: #ffffff;
      padding: 8px 14px;
      border-radius: 12px;
      box-shadow: 0 10px 25px rgba(0, 0, 0, 0.2);
      display: flex;
      align-items: center;
    }

    .brand-logo-card img {
      height: 48px;
      width: auto;
    }

    .hero-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: rgba(255, 255, 255, 0.12);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(255, 255, 255, 0.2);
      color: #e0f2fe;
      padding: 6px 16px;
      border-radius: 30px;
      font-size: 0.85rem;
      font-weight: 600;
      letter-spacing: 0.5px;
      margin-top: 35px;
      margin-bottom: 20px;
    }

    .hero-title {
      font-size: 2.5rem;
      font-weight: 800;
      line-height: 1.25;
      color: #ffffff;
      margin-bottom: 12px;
    }

    .hero-subtitle {
      font-size: 1.05rem;
      color: #94a3b8;
      max-width: 540px;
      line-height: 1.6;
    }

    /* Feature Glass Cards */
    .feature-cards-grid {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 20px;
      margin-top: 40px;
    }

    .feature-card {
      background: rgba(255, 255, 255, 0.07);
      backdrop-filter: blur(12px);
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 16px;
      padding: 22px;
      transition: transform 0.3s ease, background 0.3s ease;
    }

    .feature-card:hover {
      transform: translateY(-3px);
      background: rgba(255, 255, 255, 0.12);
    }

    .feature-icon-wrapper {
      width: 44px;
      height: 44px;
      border-radius: 12px;
      background: linear-gradient(135deg, #38bdf8 0%, #0284c7 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1.4rem;
      color: #ffffff;
      margin-bottom: 14px;
      box-shadow: 0 8px 16px rgba(2, 132, 199, 0.3);
    }

    .feature-card h5 {
      font-size: 1.05rem;
      font-weight: 700;
      color: #ffffff;
      margin-bottom: 6px;
    }

    .feature-card p {
      font-size: 0.85rem;
      color: #cbd5e1;
      margin: 0;
      line-height: 1.5;
    }

    .hero-footer-text {
      font-size: 0.85rem;
      color: #64748b;
      margin: 0;
    }

    /* Right Form Column - 42% Width */
    .form-side {
      flex: 0 0 42%;
      max-width: 42%;
      background: #ffffff;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      padding: 50px 60px;
    }

    .form-wrapper {
      width: 100%;
      max-width: 440px;
    }

    .form-header h2 {
      font-size: 2rem;
      font-weight: 800;
      color: #0f172a;
      margin-bottom: 6px;
    }

    .form-header p {
      font-size: 0.95rem;
      color: #64748b;
      margin-bottom: 32px;
    }

    .custom-input-group {
      position: relative;
      margin-bottom: 22px;
    }

    .custom-input-group label {
      display: block;
      font-size: 0.82rem;
      font-weight: 700;
      color: #334155;
      letter-spacing: 0.5px;
      text-transform: uppercase;
      margin-bottom: 8px;
    }

    .input-inner {
      position: relative;
      display: flex;
      align-items: center;
    }

    .input-inner i.input-icon {
      position: absolute;
      left: 16px;
      font-size: 1.25rem;
      color: #94a3b8;
      pointer-events: none;
      transition: color 0.2s ease;
    }

    .custom-input-control {
      width: 100%;
      height: 52px;
      padding: 12px 16px 12px 48px;
      font-size: 0.98rem;
      color: #0f172a;
      background: #f8fafc;
      border: 1.5px solid #e2e8f0;
      border-radius: 12px;
      outline: none;
      transition: all 0.25s ease;
    }

    .custom-input-control:focus {
      background: #ffffff;
      border-color: #0284c7;
      box-shadow: 0 0 0 4px rgba(2, 132, 199, 0.12);
    }

    .input-inner:focus-within i.input-icon {
      color: #0284c7;
    }

    .pass-toggle-btn {
      position: absolute;
      right: 14px;
      background: none;
      border: none;
      color: #94a3b8;
      font-size: 1.25rem;
      cursor: pointer;
      padding: 0;
      display: flex;
      align-items: center;
      transition: color 0.2s ease;
    }

    .pass-toggle-btn:hover {
      color: #0284c7;
    }

    .btn-submit-login {
      width: 100%;
      height: 54px;
      background: linear-gradient(135deg, #0284c7 0%, #0369a1 100%);
      color: #ffffff;
      border: none;
      border-radius: 12px;
      font-size: 1.05rem;
      font-weight: 700;
      letter-spacing: 0.5px;
      cursor: pointer;
      box-shadow: 0 10px 20px -5px rgba(2, 132, 199, 0.4);
      transition: all 0.3s ease;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      margin-top: 10px;
    }

    .btn-submit-login:hover {
      background: linear-gradient(135deg, #0369a1 0%, #075985 100%);
      transform: translateY(-2px);
      box-shadow: 0 14px 24px -5px rgba(2, 132, 199, 0.5);
    }

    /* Responsive Breakdown */
    @media (max-width: 992px) {
      .login-container {
        flex-direction: column;
      }
      .hero-side, .form-side {
        flex: 0 0 100%;
        max-width: 100%;
      }
      .hero-side {
        padding: 40px 30px;
      }
      .feature-cards-grid {
        grid-template-columns: 1fr;
      }
      .form-side {
        padding: 40px 25px;
      }
    }
  </style>
</head>

<body>
  <div class="login-container">
    
    <!-- Left 58% Hero Column -->
    <div class="hero-side">
      <div>
        <div class="brand-header-logos">
          <div class="brand-logo-card">
            <img src="images/pmidc.jpg" alt="PMIDC Logo">
          </div>
          <div class="brand-logo-card">
            <img src="images/logo1.png" alt="Punjab Government Logo">
          </div>
        </div>

        <div class="hero-badge">
          <i class="typcn typcn-flash"></i> Department of Local Government, Punjab
        </div>

        <h1 class="hero-title">Street Light Monitoring & Survey System</h1>
        <p class="hero-subtitle">
          Centralized administrative portal for real-time ULB streetlighting management, institutional profiles, and data analytics under PMIDC.
        </p>

        <div class="feature-cards-grid">
          <div class="feature-card">
            <div class="feature-icon-wrapper">
              <i class="typcn typcn-chart-bar"></i>
            </div>
            <h5>Live Infrastructure Monitoring</h5>
            <p>Track functional vs non-functional streetlights and energy consumption across all 137 ULBs.</p>
          </div>

          <div class="feature-card">
            <div class="feature-icon-wrapper">
              <i class="typcn typcn-document-text"></i>
            </div>
            <h5>Know Your ULB Survey</h5>
            <p>Comprehensive 110-point institutional survey engine with dynamic custom question expansion.</p>
          </div>
        </div>
      </div>

      <div class="pt-4">
        <p class="hero-footer-text">
          © 2026 Punjab Municipal Infrastructure Development Company (PMIDC). All rights reserved.
        </p>
      </div>
    </div>

    <!-- Right 42% Form Column -->
    <div class="form-side">
      <div class="form-wrapper">
        
        <div class="form-header">
          <h2>Portal Login</h2>
          <p>Enter your official credentials to access the workspace</p>
        </div>

        <?php if (!empty($error)): ?>
          <div class="alert alert-danger alert-dismissible fade show mb-4" role="alert" style="border-radius: 12px; font-size: 0.9rem;">
            <i class="typcn typcn-warning-outline mr-1"></i> <strong>Login Failed:</strong> <?php echo $error; ?>
            <button type="button" class="close" data-dismiss="alert" aria-label="Close">
              <span aria-hidden="true">&times;</span>
            </button>
          </div>
        <?php endif; ?>

        <form method="post" action="index.php">
          
          <div class="custom-input-group">
            <label for="username">Username</label>
            <div class="input-inner">
              <i class="typcn typcn-user input-icon"></i>
              <input type="text" class="custom-input-control" id="username" name="username" placeholder="Enter username" required value="<?php if(isset($_COOKIE["user_login"])) { echo htmlspecialchars($_COOKIE["user_login"]); } ?>">
            </div>
          </div>

          <div class="custom-input-group">
            <label for="password">Password</label>
            <div class="input-inner">
              <i class="typcn typcn-key input-icon"></i>
              <input type="password" class="custom-input-control" id="password" name="password" placeholder="Enter password" required value="<?php if(isset($_COOKIE["userpassword"])) { echo htmlspecialchars($_COOKIE["userpassword"]); } ?>">
              <button type="button" class="pass-toggle-btn" id="togglePasswordBtn" title="Show/Hide Password">
                <i class="typcn typcn-eye" id="eyeIcon"></i>
              </button>
            </div>
          </div>

          <div class="d-flex justify-content-between align-items-center mb-4">
            <div class="custom-control custom-checkbox">
              <input type="checkbox" class="custom-control-input" id="remember" name="remember" <?php if(isset($_COOKIE["user_login"])) { ?> checked <?php } ?>>
              <label class="custom-control-label text-muted font-weight-600" for="remember" style="font-size: 0.88rem; cursor: pointer;">Remember me</label>
            </div>
            <a href="forgot-password.php" class="text-primary font-weight-bold" style="font-size: 0.88rem; text-decoration: none;">Forgot Password?</a>
          </div>

          <button type="submit" name="submit" class="btn-submit-login">
            <i class="typcn typcn-arrow-right-outline"></i> Sign In to Portal
          </button>
        </form>

      </div>
    </div>

  </div>

  <script src="vendors/js/vendor.bundle.base.js"></script>
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
  <script>
    // Password visibility toggle
    document.getElementById('togglePasswordBtn').addEventListener('click', function () {
      var passInput = document.getElementById('password');
      var eyeIcon = document.getElementById('eyeIcon');
      if (passInput.type === 'password') {
        passInput.type = 'text';
        eyeIcon.className = 'typcn typcn-eye-outline';
      } else {
        passInput.type = 'password';
        eyeIcon.className = 'typcn typcn-eye';
      }
    });
  </script>
</body>

</html>
