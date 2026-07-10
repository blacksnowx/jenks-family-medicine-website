document.addEventListener('DOMContentLoaded', () => {
    const header = document.querySelector('[data-site-header]');
    const navToggle = document.querySelector('[data-nav-toggle]');
    const mainNav = document.querySelector('[data-main-nav]');
    const navScrim = document.querySelector('[data-nav-scrim]');
    const desktopNavQuery = window.matchMedia('(min-width: 1121px)');
    let navReturnFocus = null;

    const updateHeader = () => {
        if (header) {
            header.classList.toggle('is-scrolled', window.scrollY > 12);
        }
    };

    const closeNavigation = ({ restoreFocus = true } = {}) => {
        if (!navToggle || !mainNav || !navScrim) {
            return;
        }

        navToggle.setAttribute('aria-expanded', 'false');
        navToggle.setAttribute('aria-label', 'Open navigation');
        mainNav.classList.remove('is-open');
        navScrim.classList.remove('is-open');
        navScrim.setAttribute('aria-hidden', 'true');
        document.body.classList.remove('nav-is-open');

        if (restoreFocus && navReturnFocus instanceof HTMLElement) {
            navReturnFocus.focus();
        }
        navReturnFocus = null;
    };

    const openNavigation = () => {
        if (!navToggle || !mainNav || !navScrim) {
            return;
        }

        navReturnFocus = document.activeElement;
        navToggle.setAttribute('aria-expanded', 'true');
        navToggle.setAttribute('aria-label', 'Close navigation');
        mainNav.classList.add('is-open');
        navScrim.classList.add('is-open');
        navScrim.setAttribute('aria-hidden', 'false');
        document.body.classList.add('nav-is-open');

    };

    window.addEventListener('scroll', updateHeader, { passive: true });
    updateHeader();

    if (navToggle && mainNav && navScrim) {
        navToggle.addEventListener('click', () => {
            const isOpen = navToggle.getAttribute('aria-expanded') === 'true';
            if (isOpen) {
                closeNavigation();
            } else {
                openNavigation();
            }
        });

        navScrim.addEventListener('click', () => closeNavigation());

        mainNav.addEventListener('click', (event) => {
            if (event.target.closest('a')) {
                closeNavigation({ restoreFocus: false });
            }
        });

        document.addEventListener('keydown', (event) => {
            if (event.key === 'Escape' && navToggle.getAttribute('aria-expanded') === 'true') {
                closeNavigation();
            }

            if (event.key !== 'Tab' || navToggle.getAttribute('aria-expanded') !== 'true') {
                return;
            }

            const focusable = [
                navToggle,
                ...Array.from(mainNav.querySelectorAll('a[href], button:not([disabled])')),
            ];
            if (!focusable.length) {
                return;
            }

            const first = focusable[0];
            const last = focusable[focusable.length - 1];
            if (event.shiftKey && document.activeElement === first) {
                event.preventDefault();
                last.focus();
            } else if (!event.shiftKey && document.activeElement === last) {
                event.preventDefault();
                first.focus();
            }
        });

        const handleDesktopNav = (event) => {
            if (event.matches) {
                closeNavigation({ restoreFocus: false });
            }
        };
        desktopNavQuery.addEventListener('change', handleDesktopNav);
    }

    document.querySelectorAll('[data-dismiss-banner]').forEach((button) => {
        button.addEventListener('click', () => {
            const banner = button.closest('.site-banner');
            if (banner) {
                banner.remove();
            }
        });
    });

    const revealElements = Array.from(document.querySelectorAll('.reveal'));
    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    if (!reduceMotion && 'IntersectionObserver' in window) {
        const revealObserver = new IntersectionObserver(
            (entries, observer) => {
                entries.forEach((entry) => {
                    if (!entry.isIntersecting) {
                        return;
                    }
                    entry.target.classList.add('is-visible');
                    observer.unobserve(entry.target);
                });
            },
            { threshold: 0.08, rootMargin: '0px 0px -24px 0px' }
        );

        revealElements.forEach((element) => {
            element.classList.add('reveal-pending');
            revealObserver.observe(element);
        });
    }

    const reviewsSection = document.querySelector('[data-reviews-section]');
    if (!reviewsSection) {
        return;
    }

    const carousel = reviewsSection.querySelector('[data-reviews-carousel]');
    const track = reviewsSection.querySelector('[data-reviews-track]');
    const viewport = reviewsSection.querySelector('[data-reviews-window]');
    const prevButton = reviewsSection.querySelector('[data-reviews-prev]');
    const nextButton = reviewsSection.querySelector('[data-reviews-next]');
    const pauseButton = reviewsSection.querySelector('[data-reviews-toggle]');
    const status = reviewsSection.querySelector('[data-reviews-status]');
    const summary = reviewsSection.querySelector('[data-reviews-summary]');
    const ratingValue = reviewsSection.querySelector('[data-reviews-rating]');
    const countValue = reviewsSection.querySelector('[data-reviews-count]');
    let loaderCard = reviewsSection.querySelector('[data-reviews-loader]');
    let reviews = [];
    let reviewCards = [];
    let currentIndex = 0;
    let autoPlayTimer = null;
    let userPaused = reduceMotion;

    const AUTO_PLAY_INTERVAL = 8000;

    const setStatus = (message) => {
        if (!status) {
            return;
        }
        status.textContent = message || '';
        status.classList.toggle('is-hidden', !message);
    };

    const stopAutoPlay = () => {
        if (autoPlayTimer) {
            window.clearInterval(autoPlayTimer);
            autoPlayTimer = null;
        }
    };

    const updatePauseButton = () => {
        if (!pauseButton) {
            return;
        }
        pauseButton.textContent = userPaused ? 'Play reviews' : 'Pause reviews';
        pauseButton.setAttribute('aria-pressed', userPaused ? 'true' : 'false');
        pauseButton.classList.toggle('is-hidden', reviews.length <= 1);
    };

    const updateTransform = () => {
        if (!viewport || !track || !reviews.length) {
            return;
        }
        const width = viewport.getBoundingClientRect().width;
        track.style.transform = `translateX(-${width * currentIndex}px)`;
        reviewCards.forEach((card, index) => {
            const isCurrent = index === currentIndex;
            card.setAttribute('aria-hidden', isCurrent ? 'false' : 'true');
            const links = card.querySelectorAll('a');
            links.forEach((link) => {
                link.tabIndex = isCurrent ? 0 : -1;
            });
        });
    };

    const goTo = (index, { announce = false } = {}) => {
        if (!reviews.length) {
            return;
        }
        currentIndex = (index + reviews.length) % reviews.length;
        updateTransform();
        if (announce) {
            setStatus(`Showing review ${currentIndex + 1} of ${reviews.length}.`);
        } else {
            setStatus('');
        }
    };

    const startAutoPlay = () => {
        stopAutoPlay();
        if (userPaused || reviews.length <= 1 || document.hidden) {
            return;
        }
        autoPlayTimer = window.setInterval(() => goTo(currentIndex + 1), AUTO_PLAY_INTERVAL);
    };

    const removeReviewsSection = () => {
        stopAutoPlay();
        window.removeEventListener('resize', updateTransform);
        reviewsSection.remove();
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
        } catch (_error) {
            return 'Recent review';
        }
    };

    const createStars = (rating = 5) => {
        const wrapper = document.createElement('div');
        wrapper.className = 'review-card__stars';
        const normalizedRating = Math.max(0, Math.min(5, Math.round(Number(rating) || 5)));
        wrapper.setAttribute('aria-label', `${normalizedRating} out of 5 stars`);

        for (let index = 0; index < 5; index += 1) {
            const star = document.createElement('span');
            star.className = 'review-card__star';
            star.textContent = index < normalizedRating ? '★' : '☆';
            if (index >= normalizedRating) {
                star.classList.add('review-card__star--empty');
            }
            wrapper.appendChild(star);
        }
        return wrapper;
    };

    const createReviewCard = (review, index) => {
        const card = document.createElement('article');
        card.className = 'review-card';
        card.setAttribute('aria-label', `Patient review ${index + 1} of ${reviews.length}`);

        const quote = document.createElement('div');
        quote.className = 'review-card__quote';
        quote.appendChild(createStars(review.rating));

        const text = document.createElement('p');
        text.className = 'review-card__text';
        text.textContent = typeof review.text === 'string' ? review.text.trim() : '';
        quote.appendChild(text);

        const footer = document.createElement('div');
        footer.className = 'review-card__footer';
        const author = document.createElement('div');
        author.className = 'review-card__author';
        const avatar = document.createElement('div');
        avatar.className = 'review-card__avatar';
        const authorName = typeof review.author_name === 'string' && review.author_name.trim()
            ? review.author_name.trim()
            : 'Google user';

        if (review.profile_photo_url) {
            const image = document.createElement('img');
            image.src = review.profile_photo_url;
            image.alt = '';
            image.loading = 'lazy';
            avatar.appendChild(image);
        } else {
            avatar.textContent = authorName.charAt(0).toUpperCase();
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
        meta.append(name, date);
        author.append(avatar, meta);

        const source = document.createElement('span');
        source.className = 'review-card__source';
        source.textContent = 'Google review';
        footer.append(author, source);
        card.append(quote, footer);
        return card;
    };

    const renderReviews = (items) => {
        reviews = items;
        track.replaceChildren();
        const fragment = document.createDocumentFragment();
        reviews.forEach((review, index) => fragment.appendChild(createReviewCard(review, index)));
        track.appendChild(fragment);
        reviewCards = Array.from(track.querySelectorAll('.review-card'));
        currentIndex = 0;
        updateTransform();

        const disableControls = reviews.length <= 1;
        if (prevButton) prevButton.disabled = disableControls;
        if (nextButton) nextButton.disabled = disableControls;
        updatePauseButton();
        startAutoPlay();
    };

    const applySummary = (data) => {
        const rating = Number(data.rating);
        if (summary && ratingValue && Number.isFinite(rating)) {
            ratingValue.textContent = rating.toFixed(1);
            summary.classList.remove('is-hidden');
        }

        const total = Number(data.total_ratings);
        if (countValue && Number.isFinite(total)) {
            countValue.textContent = total.toLocaleString();
        }
    };

    if (prevButton) {
        prevButton.addEventListener('click', () => {
            stopAutoPlay();
            goTo(currentIndex - 1, { announce: true });
        });
    }

    if (nextButton) {
        nextButton.addEventListener('click', () => {
            stopAutoPlay();
            goTo(currentIndex + 1, { announce: true });
        });
    }

    if (pauseButton) {
        pauseButton.addEventListener('click', () => {
            userPaused = !userPaused;
            updatePauseButton();
            if (userPaused) {
                stopAutoPlay();
            } else {
                startAutoPlay();
            }
        });
    }

    if (carousel) {
        carousel.addEventListener('mouseenter', stopAutoPlay);
        carousel.addEventListener('mouseleave', startAutoPlay);
        carousel.addEventListener('focusin', stopAutoPlay);
        carousel.addEventListener('focusout', startAutoPlay);
    }

    document.addEventListener('visibilitychange', () => {
        if (document.hidden) {
            stopAutoPlay();
        } else {
            startAutoPlay();
        }
    });
    window.addEventListener('resize', updateTransform);

    fetch('/api/google-reviews')
        .then(async (response) => {
            const data = await response.json().catch(() => ({ reviews: [] }));
            if (!response.ok) {
                throw new Error(data.error || 'Reviews are unavailable.');
            }
            return data;
        })
        .then((data) => {
            const items = Array.isArray(data.reviews)
                ? data.reviews.filter((review) => review && typeof review.text === 'string' && review.text.trim())
                : [];

            if (!items.length) {
                removeReviewsSection();
                return;
            }

            if (loaderCard) {
                loaderCard.remove();
                loaderCard = null;
            }
            renderReviews(items);
            applySummary(data);
            setStatus('');
        })
        .catch(() => removeReviewsSection());
});
