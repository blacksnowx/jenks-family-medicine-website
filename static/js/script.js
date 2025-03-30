document.addEventListener('DOMContentLoaded', () => {
    const header = document.getElementById('main-header');
    const mobileNavToggle = document.querySelector('.mobile-nav-toggle');
    const mainNav = document.getElementById('main-nav'); // Make sure your nav element has id="main-nav"

    // --- Sticky Header on Scroll ---
    const handleScroll = () => {
        if (window.scrollY > 50) { // Adjust pixel value as needed
            header.classList.add('scrolled');
        } else {
            header.classList.remove('scrolled');
        }
    };

    // Add scroll event listener
    window.addEventListener('scroll', handleScroll);
    // Initial check in case page loads already scrolled
    handleScroll();


    // --- Mobile Navigation Toggle ---
    if (mobileNavToggle && mainNav) {
        mobileNavToggle.addEventListener('click', () => {
            mainNav.classList.toggle('active');
            mobileNavToggle.classList.toggle('active'); // Toggle icon state (e.g., change hamburger to X)

            // Optional: Prevent body scroll when mobile menu is open
            // document.body.classList.toggle('mobile-nav-active');
        });
    } else {
        console.warn('Mobile nav toggle button or main navigation element not found.');
    }


    // --- Intersection Observer for Scroll Animations ---
    const revealElements = document.querySelectorAll('.reveal');

    const revealObserver = new IntersectionObserver((entries, observer) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('active');
                // Optional: Stop observing once revealed
                // observer.unobserve(entry.target);
            } else {
                 // Optional: Remove class if you want animation to reverse when scrolling up
                 // entry.target.classList.remove('active');
            }
        });
    }, {
        root: null, // relative to the viewport
        threshold: 0.1, // trigger when 10% of the element is visible
        // rootMargin: '0px 0px -50px 0px' // Optional: Adjust trigger point
    });

    revealElements.forEach(el => {
        revealObserver.observe(el);
    });

});