<?php
session_start();
error_reporting(0);
include('includes/config.php');
error_reporting(0);

if(isset($_POST['submit']))
  {
    $contactno=$_SESSION['contactno'];
    $email=$_SESSION['email'];
    $password=md5($_POST['newpassword']);

        $query=mysqli_query($con,"update streetlightlogin set Password='$password'  where  Email='$email' && mobile_no='$contactno' ");
   if($query)
   {
echo "<script>alert('Password successfully changed');
window.location.href = 'index.php';
</script>";
session_destroy();
   }
  
  }
  ?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reset Password || Street Light Monitoring System</title>
    <link rel="icon" type="image/png" href="images/pmidc.jpg" />
    
    <!-- Bootstrap 5 -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- Google Fonts -->
    <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <!-- FontAwesome -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    
    <script type="text/javascript">
    function checkpass() {
        if(document.changepassword.newpassword.value != document.changepassword.confirmpassword.value) {
            alert('New Password and Confirm Password field does not match');
            document.changepassword.confirmpassword.focus();
            return false;
        }
        return true;
    } 
    </script>
    
    <style>
        body {
            font-family: 'Manrope', sans-serif;
            background-color: #ffffff;
            height: 100vh;
            overflow: hidden;
            margin: 0;
        }
        .split-layout {
            display: flex;
            height: 100vh;
            width: 100vw;
        }
        
        /* Left Side: Branding */
        .brand-section {
            width: 55%;
            background: linear-gradient(135deg, #0b1329 0%, #1e3a5f 50%, #006eb3 100%);
            color: white;
            padding: 4rem 5rem;
            display: flex;
            flex-direction: column;
            position: relative;
        }
        .logos {
            display: flex;
            gap: 15px;
            margin-bottom: 2.5rem;
        }
        .logo-box {
            background: white;
            border-radius: 10px;
            padding: 8px 12px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .logo-box img {
            height: 35px;
        }
        .dept-badge {
            background: rgba(255, 255, 255, 0.15);
            border: 1px solid rgba(255, 255, 255, 0.2);
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            width: max-content;
            margin-bottom: 2rem;
            backdrop-filter: blur(4px);
        }
        .dept-badge i { margin-right: 6px; }
        
        .main-title {
            font-size: 2.5rem;
            font-weight: 800;
            line-height: 1.2;
            margin-bottom: 1.2rem;
            letter-spacing: -0.5px;
        }
        .sub-title {
            font-size: 1rem;
            color: rgba(255, 255, 255, 0.7);
            max-width: 80%;
            line-height: 1.6;
            margin-bottom: 3rem;
            font-weight: 500;
        }
        
        .feature-cards {
            display: flex;
            gap: 20px;
        }
        .feature-card {
            background: rgba(255, 255, 255, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 12px;
            padding: 1.5rem;
            flex: 1;
            backdrop-filter: blur(10px);
        }
        .feature-icon {
            background: #2094f3;
            width: 32px; height: 32px;
            border-radius: 8px;
            display: flex; align-items: center; justify-content: center;
            margin-bottom: 1rem;
        }
        .feature-card h5 { font-size: 1rem; font-weight: 700; margin-bottom: 8px; }
        .feature-card p { font-size: 0.8rem; color: rgba(255, 255, 255, 0.6); margin: 0; line-height: 1.5; }
        
        .copyright {
            position: absolute;
            bottom: 2rem;
            font-size: 0.75rem;
            color: rgba(255, 255, 255, 0.4);
        }

        /* Right Side: Login Form */
        .login-section {
            width: 45%;
            background: white;
            display: flex;
            flex-direction: column;
            justify-content: center;
            padding: 4rem;
        }
        .login-wrapper {
            max-width: 400px;
            margin: 0 auto;
            width: 100%;
        }
        .login-title {
            font-size: 1.8rem;
            font-weight: 800;
            color: #0f172a;
            margin-bottom: 5px;
            letter-spacing: -0.5px;
        }
        .login-subtitle {
            font-size: 0.85rem;
            color: #64748b;
            margin-bottom: 2.5rem;
            font-weight: 500;
        }
        
        .form-label {
            font-size: 0.75rem;
            font-weight: 700;
            color: #334155;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }
        .input-group {
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            overflow: hidden;
            transition: all 0.2s;
        }
        .input-group:focus-within {
            border-color: #0284c7;
            box-shadow: 0 0 0 3px rgba(2, 132, 199, 0.1);
        }
        .input-group-text {
            background: transparent;
            border: none;
            color: #94a3b8;
            padding-left: 1rem;
        }
        .form-control {
            background: transparent;
            border: none;
            padding: 0.75rem;
            font-size: 0.9rem;
            font-weight: 500;
            color: #0f172a;
            box-shadow: none !important;
        }
        .form-control::placeholder { color: #cbd5e1; font-weight: 400; }
        
        .login-options {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 1.5rem;
            margin-bottom: 2rem;
            font-size: 0.8rem;
        }
        .forgot-link {
            color: #6366f1;
            font-weight: 600;
            text-decoration: none;
        }
        .forgot-link:hover { text-decoration: underline; }
        
        .btn-login {
            background: #0284c7;
            color: white;
            border: none;
            width: 100%;
            padding: 0.85rem;
            border-radius: 8px;
            font-weight: 700;
            font-size: 0.9rem;
            transition: all 0.3s;
            box-shadow: 0 4px 12px rgba(2, 132, 199, 0.2);
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 8px;
        }
        .btn-login:hover {
            background: #0369a1;
            transform: translateY(-1px);
            box-shadow: 0 6px 15px rgba(2, 132, 199, 0.3);
        }
        
        @media (max-width: 992px) {
            .brand-section { display: none; }
            .login-section { width: 100%; padding: 2rem; }
        }
    </style>
</head>
<body>
    <div class="split-layout">
        <!-- Left Side: Branding -->
        <div class="brand-section">
            <div class="logos">
                <div class="logo-box">
                    <img src="images/pmidc.jpg" alt="PMIDC Logo" onerror="this.src='images/logo1.png';">
                </div>
                <div class="logo-box">
                    <img src="images/logo1.png" alt="Gov Logo" onerror="this.src='images/pmidc.jpg';">
                </div>
            </div>
            
            <div class="dept-badge">
                <i class="fa-solid fa-bolt"></i> Department of Local Government, Punjab
            </div>
            
            <h1 class="main-title">Street Light Monitoring & Survey System</h1>
            <p class="sub-title">Centralized administrative portal for real-time ULB streetlighting management, institutional profiles, and data analytics under PMIDC.</p>
            
            <div class="feature-cards">
                <div class="feature-card">
                    <div class="feature-icon"><i class="fa-solid fa-chart-line fa-sm text-white"></i></div>
                    <h5>Live Infrastructure Monitoring</h5>
                    <p>Track functional vs non-functional streetlights and energy consumption across all 137 ULBs.</p>
                </div>
                <div class="feature-card">
                    <div class="feature-icon"><i class="fa-solid fa-clipboard-list fa-sm text-white"></i></div>
                    <h5>Know Your ULB Survey</h5>
                    <p>Comprehensive 110-point institutional survey engine with dynamic custom question expansion.</p>
                </div>
            </div>
            
            <div class="copyright">
                &copy; 2026 Punjab Municipal Infrastructure Development Company (PMIDC). All rights reserved.
            </div>
        </div>
        
        <!-- Right Side: Login -->
        <div class="login-section">
            <div class="login-wrapper">
                <h2 class="login-title">Create New Password</h2>
                <p class="login-subtitle">Please enter your new password below.</p>
                
                <form method="post" name="changepassword" onsubmit="return checkpass();">
                    <div class="mb-4">
                        <label class="form-label">NEW PASSWORD</label>
                        <div class="input-group">
                            <span class="input-group-text"><i class="fa-solid fa-lock"></i></span>
                            <input type="password" class="form-control" placeholder="••••••••" name="newpassword" required>
                        </div>
                    </div>
                    
                    <div>
                        <label class="form-label">CONFIRM PASSWORD</label>
                        <div class="input-group">
                            <span class="input-group-text"><i class="fa-solid fa-lock"></i></span>
                            <input type="password" class="form-control" placeholder="••••••••" name="confirmpassword" required>
                        </div>
                    </div>
                    
                    <div class="login-options">
                        <a href="index.php" class="forgot-link"><i class="fa-solid fa-arrow-left"></i> Back to Sign In</a>
                    </div>
                    
                    <button type="submit" class="btn btn-login" name="submit">
                        <i class="fa-solid fa-check-circle"></i> Save New Password
                    </button>
                </form>
            </div>
        </div>
    </div>
</body>
</html>
