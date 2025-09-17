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

    // --- Google Reviews Carousel ---
    const reviewsSection = document.querySelector('[data-reviews-section]');
    if (reviewsSection) {
        const carousel = reviewsSection.querySelector('[data-reviews-carousel]');
        const track = reviewsSection.querySelector('[data-reviews-track]');
        const viewport = reviewsSection.querySelector('[data-reviews-window]');
        const prevButton = reviewsSection.querySelector('[data-reviews-prev]');
        const nextButton = reviewsSection.querySelector('[data-reviews-next]');
        const status = reviewsSection.querySelector('[data-reviews-status]');
        let loaderCard = reviewsSection.querySelector('[data-reviews-loader]');
        const summary = reviewsSection.querySelector('[data-reviews-summary]');
        const ratingValue = reviewsSection.querySelector('[data-reviews-rating]');
        const countValue = reviewsSection.querySelector('[data-reviews-count]');

        let sectionRemoved = false;

        let reviews = [];
        let currentIndex = 0;
        let autoPlayTimer = null;

        const AUTO_PLAY_INTERVAL = 8000;

        const setStatus = (message) => {
            if (sectionRemoved || !status) {
                return;
            }
            if (message) {
                status.textContent = message;
                status.classList.remove('is-hidden');
            } else {
                status.textContent = '';
                status.classList.add('is-hidden');
            }
        };

        const removeLoader = () => {
            if (loaderCard && loaderCard.parentElement) {
                loaderCard.parentElement.removeChild(loaderCard);
            }
            loaderCard = null;
        };

        const formatTimestamp = (timestamp) => {
            if (!timestamp) {
                return 'Recent review';
            }
            try {
                return new Intl.DateTimeFormat(undefined, {
                    year: 'numeric',
                    month: 'short',
                }).format(new Date(timestamp * 1000));
            } catch (error) {
                return 'Recent review';
            }
        };

        const createStarContainer = (rating = 5) => {
            const wrapper = document.createElement('div');
            wrapper.className = 'review-card__stars';
            const normalizedRating = Math.round(Number(rating) || 5);
            wrapper.setAttribute('aria-label', `${normalizedRating} out of 5 stars`);

            for (let index = 0; index < 5; index += 1) {
                const star = document.createElement('span');
                star.className = 'review-card__star';
                if (index >= normalizedRating) {
                    star.classList.add('review-card__star--empty');
                    star.textContent = '☆';
                } else {
                    star.textContent = '★';
                }
                wrapper.appendChild(star);
            }

            return wrapper;
        };

        const createReviewCard = (review) => {
            const card = document.createElement('article');
            card.className = 'review-card';

            const quote = document.createElement('div');
            quote.className = 'review-card__quote';
            quote.appendChild(createStarContainer(review.rating));

            const text = document.createElement('p');
            text.className = 'review-card__text';
            const reviewText = typeof review.text === 'string' ? review.text.trim() : '';
            text.textContent = reviewText;
            quote.appendChild(text);

            const footer = document.createElement('div');
            footer.className = 'review-card__footer';

            const author = document.createElement('div');
            author.className = 'review-card__author';

            const avatar = document.createElement('div');
            avatar.className = 'review-card__avatar';
            const rawAuthorName = typeof review.author_name === 'string' ? review.author_name.trim() : '';
            const authorName = rawAuthorName || 'Google user';
            if (review.profile_photo_url) {
                const img = document.createElement('img');
                img.src = review.profile_photo_url;
                img.alt = `${authorName}'s Google profile photo`;
                avatar.appendChild(img);
            } else {
                const initial = authorName.charAt(0) || 'G';
                avatar.textContent = initial.toUpperCase();
            }

            const meta = document.createElement('div');
            meta.className = 'review-card__author-meta';

            const name = document.createElement('span');
            name.className = 'review-card__name';
            if (review.author_url) {
                const link = document.createElement('a');
                link.href = review.author_url;
                link.target = '_blank';
                link.rel = 'noopener noreferrer';
                link.textContent = authorName;
                name.appendChild(link);
            } else {
                name.textContent = authorName;
            }

            const date = document.createElement('span');
            date.className = 'review-card__date';
            date.textContent = review.relative_time_description || formatTimestamp(review.time);

            meta.appendChild(name);
            meta.appendChild(date);

            author.appendChild(avatar);
            author.appendChild(meta);

            const source = document.createElement('span');
            source.className = 'review-card__source';
            source.innerHTML = `
                <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                    <path fill="#EA4335" d="M12 11.8v3.6h5.1c-.2 1.3-1.5 3.8-5.1 3.8-3.1 0-5.6-2.6-5.6-5.8s2.5-5.8 5.6-5.8c1.8 0 3.1.8 3.8 1.5l2.6-2.5C16.7 5 14.6 4 12 4 6.9 4 2.8 8.1 2.8 13.2S6.9 22.4 12 22.4c6.5 0 8.9-4.6 8.9-7.8 0-.5-.1-1-.1-1.4H12z"></path>
                    <path fill="#34A853" d="M3.3 7.7l3 2.2C7.6 7.5 9.6 6.1 12 6.1c1.8 0 3.1.8 3.8 1.5l2.6-2.5C16.7 3.7 14.6 2.7 12 2.7 8.4 2.7 5.3 4.7 3.3 7.7z"></path>
                    <path fill="#FBBC05" d="M12 22.4c3.6 0 6.1-1.2 7.8-3l-3.6-2.7c-1 .7-2.4 1.2-4.2 1.2-3.3 0-6-2.4-6.6-5.6l-3.1 2.4c1.7 3.5 5 5.7 9.7 5.7z"></path>
                    <path fill="#4285F4" d="M20.9 14.6c-.2-1.1-.6-2-1-2.8H12v3.6h5.1c-.1.8-.6 1.9-1.6 2.6l3.6 2.7c1-1.1 1.7-2.6 1.7-4.1 0-.7-.1-1.3-.3-2z"></path>
                </svg>
                <span>Google Reviews</span>
            `;

            footer.appendChild(author);
            footer.appendChild(source);

            card.appendChild(quote);
            card.appendChild(footer);

            return card;
        };

        const hideReviewsSection = () => {
            if (sectionRemoved) {
                return;
            }

            sectionRemoved = true;
            removeLoader();
            stopAutoPlay();
            reviews = [];

            if (summary) {
                summary.classList.add('is-hidden');
            }

            if (status) {
                status.textContent = '';
                status.classList.add('is-hidden');
            }

            window.removeEventListener('resize', updateTransform);

            if (
                typeof revealObserver !== 'undefined' &&
                revealObserver &&
                typeof revealObserver.unobserve === 'function'
            ) {
                try {
                    revealObserver.unobserve(reviewsSection);
                } catch (observerError) {
                    console.warn('Unable to unobserve reviews section', observerError);
                }
            }

            if (reviewsSection) {
                reviewsSection.classList.add('is-hidden');
                reviewsSection.setAttribute('hidden', '');

                if (reviewsSection.parentElement) {
                    reviewsSection.parentElement.removeChild(reviewsSection);
                }
            }
        };

        const updateTransform = () => {
            if (sectionRemoved || !viewport || !track) {
                return;
            }
            const width = viewport.getBoundingClientRect().width;
            track.style.transform = `translateX(-${width * currentIndex}px)`;
        };

        const goTo = (index) => {
            if (sectionRemoved || !reviews.length) {
                return;
            }
            const total = reviews.length;
            currentIndex = (index + total) % total;
            updateTransform();
        };

        const updateControls = () => {
            if (sectionRemoved) {
                return;
            }
            const shouldDisable = reviews.length <= 1;
            if (prevButton) {
                prevButton.disabled = shouldDisable;
            }
            if (nextButton) {
                nextButton.disabled = shouldDisable;
            }
        };

        const stopAutoPlay = () => {
            if (autoPlayTimer) {
                window.clearInterval(autoPlayTimer);
                autoPlayTimer = null;
            }
        };

        const startAutoPlay = () => {
            stopAutoPlay();
            if (sectionRemoved || reviews.length <= 1) {
                return;
            }
            autoPlayTimer = window.setInterval(() => {
                goTo(currentIndex + 1);
            }, AUTO_PLAY_INTERVAL);
        };

        const renderReviews = (items) => {
            if (sectionRemoved) {
                return;
            }
            reviews = items;
            track.innerHTML = '';

            const fragment = document.createDocumentFragment();
            reviews.forEach((review) => {
                fragment.appendChild(createReviewCard(review));
            });

            track.appendChild(fragment);
            currentIndex = 0;
            updateTransform();
            updateControls();
            startAutoPlay();
        };

        const applySummary = (data) => {
            if (sectionRemoved || !summary || !ratingValue) {
                return;
            }

            const rating = Number(data.rating);
            if (Number.isFinite(rating)) {
                ratingValue.textContent = rating.toFixed(1);
                summary.classList.remove('is-hidden');
            }

            if (countValue && data.total_ratings !== undefined && data.total_ratings !== null) {
                const total = Number(data.total_ratings);
                if (Number.isFinite(total)) {
                    countValue.textContent = total.toLocaleString();
                }
            }
        };

        const handleSuccess = (data) => {
            removeLoader();

            const fetchedReviews = Array.isArray(data.reviews)
                ? data.reviews.filter(
                      (item) => item && typeof item.text === 'string' && item.text.trim()
                  )
                : [];

            if (!fetchedReviews.length) {
                hideReviewsSection();
                return;
            }

            renderReviews(fetchedReviews);
            applySummary(data);
            setStatus('');
        };

        const handleError = (_message) => {
            hideReviewsSection();
        };

        if (prevButton) {
            prevButton.addEventListener('click', () => {
                if (!reviews.length) {
                    return;
                }
                stopAutoPlay();
                goTo(currentIndex - 1);
                startAutoPlay();
            });
        }

        if (nextButton) {
            nextButton.addEventListener('click', () => {
                if (!reviews.length) {
                    return;
                }
                stopAutoPlay();
                goTo(currentIndex + 1);
                startAutoPlay();
            });
        }

        if (carousel) {
            carousel.addEventListener('mouseenter', stopAutoPlay);
            carousel.addEventListener('mouseleave', startAutoPlay);
            carousel.addEventListener('focusin', stopAutoPlay);
            carousel.addEventListener('focusout', startAutoPlay);
        }

        window.addEventListener('resize', updateTransform);

        fetch('/api/google-reviews')
            .then(async (response) => {
                const data = await response
                    .json()
                    .catch(() => ({ reviews: [], error: 'Unable to load Google reviews.' }));

                if (!response.ok) {
                    const message = data.error || 'Unable to load Google reviews at this time.';
                    throw new Error(message);
                }

                return data;
            })
            .then((data) => {
                handleSuccess(data);
            })
            .catch((error) => {
                console.error('Failed to fetch Google reviews', error);
                handleError(error.message || 'Unable to load Google reviews at this time.');
            });
    }

});
