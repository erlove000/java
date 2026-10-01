-- Create Categories Table
CREATE TABLE IF NOT EXISTS `ulb_categories` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `category_name` VARCHAR(255) NOT NULL,
  `category_code` VARCHAR(100) NOT NULL UNIQUE,
  `icon_class` VARCHAR(100) DEFAULT 'typcn-folder',
  `sort_order` INT DEFAULT 0,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Create Dynamic Questions Table
CREATE TABLE IF NOT EXISTS `ulb_questions` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `category_id` INT NOT NULL,
  `question_code` VARCHAR(50) DEFAULT NULL,
  `question_text` TEXT NOT NULL,
  `input_type` VARCHAR(50) NOT NULL DEFAULT 'text', -- text, number, select_one, select_multiple, decimal
  `choice_options` TEXT DEFAULT NULL, -- Comma-separated options for dropdowns/checkboxes
  `is_mandatory` TINYINT(1) DEFAULT 0,
  `sort_order` INT DEFAULT 0,
  `is_active` TINYINT(1) DEFAULT 1,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (`category_id`) REFERENCES `ulb_categories`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Create Dynamic Question Responses Table
CREATE TABLE IF NOT EXISTS `ulb_question_responses` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `submission_id` INT NOT NULL,
  `town_id` INT NOT NULL,
  `question_id` INT NOT NULL,
  `response_value` TEXT DEFAULT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (`question_id`) REFERENCES `ulb_questions`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed Default Categories if empty
INSERT IGNORE INTO `ulb_categories` (`id`, `category_name`, `category_code`, `icon_class`, `sort_order`) VALUES
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
(13, 'WASH (Water & Sanitation)', 'wash', 'typcn-waves', 13);
