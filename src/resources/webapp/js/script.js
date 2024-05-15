const RESULTS_PER_PAGE = 10;
let currentPage = 0;
let isSearchPressed = false;
let isFetchingDone = false;
function clearSearchResults() {
    const resultsContainer = document.getElementById('resultsContainer');
    resultsContainer.innerHTML = '';
    currentPage = 0;
    isSearchPressed = false;
}

function clearInput() {
    const textarea = document.getElementById('searchQuery');
    textarea.value = '';
    textarea.style.height = '50px';
    const searchBox = document.getElementsByClassName('search-box');
    searchBox.item(0).style.height = '50px';
}

function refreshPage() {
    clearInput();
    clearSearchResults();
    // window.location.reload();
}


function fetchSearchResults() {
    let query = document.getElementById('searchQuery').value.trim();
    if (query) {
        clearSearchResults();
        isSearchPressed = true;
        hideAutocomplete();
        showLoadingWrapper();
        fetchMoreResults(true);
        window.scroll({
            top: 0,
            left: 0,
            behavior: 'smooth'
        });
    } else {
        alert('Please enter a search term');
    }
}

function displayResults(data, reset = false) {
    const resultsContainer = document.getElementById('resultsContainer');

    if (data.length === 0 && reset) {
        resultsContainer.innerHTML = '<p>No results found.</p>';
        return;
    }

    addResults(data, resultsContainer);

    if (isSearchPressed) {
        setupResultHover();
    }
}

function addResults(data, resultsContainer) {
    data.forEach(item => {
        const resultItem = document.createElement('div');
        resultItem.className = 'result-item';

        const hostnameDiv = document.createElement('div');
        hostnameDiv.className = 'result-hostname';
        hostnameDiv.innerHTML = item.hostname;

        const urlDiv = document.createElement('div');
        urlDiv.className = 'result-url';
        urlDiv.innerHTML = item.url;

        const titleDiv = document.createElement('div');
        titleDiv.className = 'result-title';

        // Create an anchor tag and set its properties
        const titleLink = document.createElement('a');
        titleLink.className = 'result-link';
        titleLink.href = item.url;
        titleLink.innerHTML = item.title;
        titleLink.style.color = '#1a0dab'; // Set the link color
        titleLink.style.textDecoration = 'none';
        titleLink.target = '_blank'; // Opens the link in a new tab
        titleLink.setAttribute('data-row-key', item.rowKey);

        // Append the anchor tag to the title div
        titleDiv.appendChild(titleLink);

        const snippetDiv = document.createElement('div');
        snippetDiv.className = 'result-snippet';
        snippetDiv.innerHTML = item.snippet;

        // Append all parts to the result item
        resultItem.appendChild(hostnameDiv);
        resultItem.appendChild(urlDiv);
        resultItem.appendChild(titleDiv);
        resultItem.appendChild(snippetDiv);

        // Append the result item to the results container
        resultsContainer.appendChild(resultItem);
    });
}

function fetchMoreResults(reset) {
    if (reset) {
        currentPage = 0;
    }
    const query = document.getElementById('searchQuery').value.trim();
    const offset = currentPage * RESULTS_PER_PAGE;
    // console.log("called fetch more results, page: " + currentPage);
    isFetchingDone = false;

    fetch(`/search?q=${encodeURIComponent(query)}`, {
        method: 'GET',
        headers: {
            'X-Page-Number': `${offset}`,
        }
    })
    .then(response => response.json())
    .then(data => {
        displayResults(data.results, reset);
        // if (data.results.length < RESULTS_PER_PAGE) {
        //     isSearchPressed = false;
        // }
    })
    .catch(error => {
        console.error('Error fetching search results:', error);
        alert('Failed to fetch search results');
    })
    .finally(() => {
        hideLoadingWrapper();
        isFetchingDone = true;
        currentPage++;
    });
}

window.addEventListener('scroll', () => {
    // console.log("doc body: " +document.body.offsetHeight);
    // console.log("window height: " + window.innerHeight);
    // console.log("scrollY: " + window.scrollY);
    // console.log("is searching: " + isSearchPressed);
    // console.log("is fetching done: " + isFetchingDone);
    // console.log("");
    if (isSearchPressed && isFetchingDone && window.innerHeight + window.scrollY >= document.body.offsetHeight) {
        fetchMoreResults(false);
    }
});

document.getElementById('searchQuery').addEventListener('keypress', function(event) {
    if (event.key === 'Enter') {
        event.preventDefault();
        fetchSearchResults();
    }
});

function autoExpand(textarea) {
    textarea.style.height = '50px';
    textarea.parentNode.style.height = '50px';
    // Calculate if the content overflows a single line (exceeding scrollHeight)
    if (textarea.scrollHeight > 0) {
        // Set the height to scrollHeight if the content overflows
        textarea.style.height = Math.max(textarea.scrollHeight, 50) + 'px';
        textarea.parentNode.style.height = Math.max(textarea.scrollHeight, 50) + 'px';
        if (textarea.scrollHeight > 50) {
            textarea.style.paddingBottom = '15px';
        }
    } else {
        textarea.style.height = '50px';
        textarea.parentNode.style.height = '50px';
    }
}

/* Hover and show cached page */

function setupResultHover() {
    const resultTitles = document.querySelectorAll('.result-title a');

    resultTitles.forEach(title => {
        let hoverTimer;

        title.addEventListener('mouseenter', function() {
            const rowKey = this.getAttribute('data-row-key');
            hoverTimer = setTimeout(() => {
                fetchHoveredContent(this, rowKey);
            }, 500); // Delay appearance to 500ms
        });

        title.addEventListener('mouseleave', function(event) {
            // Check if the mouse is moving to the popup
            if (!event.relatedTarget || !event.relatedTarget.classList.contains('popup')) {
                clearTimeout(hoverTimer);
                hidePopup(this);
            }
        });
    });
}

function fetchHoveredContent(element, rowKey) {
    fetch(`/preview?r=${encodeURIComponent(rowKey)}`, {
        method: 'GET'
    })
        .then(response => response.json())
        .then(data => {
            if (data && data.page) {
                showPopup(element, data.page);
            }
        })
        .catch(error => {
            console.error('Error fetching content:', error);
            alert('Failed to fetch content');
        });
}

function showPopup(element, htmlContent) {
    let popup = element.parentNode.querySelector('.popup');
    if (!popup) {
        // Create popup if it does not exist
        popup = document.createElement('div');
        popup.className = 'popup';
        element.parentNode.appendChild(popup);

        // Setup mouse events on the popup
        popup.addEventListener('mouseenter', function() {
            clearTimeout(this.hideTimer);
        });

        popup.addEventListener('mouseleave', function() {
            this.hideTimer = setTimeout(() => {
                this.style.display = 'none';
            }, 300); // Delay hiding the popup after mouse leaves
        });
    }
    // Create an iframe to hold the full HTML content
    const iframe = document.createElement('iframe');
    iframe.style.width = '100%';
    iframe.style.height = '100%';
    iframe.style.border = 'none';

    popup.innerHTML = '';
    popup.appendChild(iframe);
    popup.style.display = 'block';

    // Write the full HTML content to the iframe
    let doc = iframe.contentDocument || iframe.contentWindow.document;
    doc.open();
    doc.write("<div><h3>Preview</h3></div>");
    doc.write(htmlContent);
    doc.close();
}

function hidePopup(element) {
    let popup = element.parentNode.querySelector('.popup');
    if (popup && !popup.contains(event.relatedTarget)) {
        popup.style.display = 'none';
    }
}

document.addEventListener('DOMContentLoaded', function () {
    let searchBox = document.getElementById('searchBox');
    window.scrollTo(0, 0);
    window.addEventListener('scroll', function () {
        if (window.scrollY > 184) {
            searchBox.classList.add('sticky');
        } else {
            searchBox.classList.remove('sticky');
        }
    });
});

/* autocomplete functions */
document.getElementById('searchQuery').addEventListener('input', function() {
    autoExpand(this);
    const input = this.value;
    if (input.includes(" ")) {
        autocomplete(input.substring(input.lastIndexOf(" ") + 1));
    } else {
        autocomplete(input);
    }
});

function autocomplete(searchText) {
    const trimmedText = searchText.trim();

    if (trimmedText.length === 0) {
        hideAutocomplete();
        return;
    }

    if (searchText.length > 0) {
        fetch(`/autocomplete?p=${encodeURIComponent(searchText)}`)
            .then(response => response.json())
            .then(data => {
                if (data.length > 1) {
                    displayAutocomplete(data);
                } else {
                    hideAutocomplete();
                }
            })
            .catch(error => {
                console.error('Error:', error);
                hideAutocomplete();
            });
    }
}


function displayAutocomplete(suggestions) {
    const autocompleteList = document.getElementById('autocompleteList');
    const searchQuery = document.getElementById('searchQuery');
    autocompleteList.innerHTML = '';
    autocompleteList.style.top = searchQuery.style.height;
    suggestions.forEach((item) => {
        const div = document.createElement('div');
        div.innerText = item;
        div.onclick = function() {
            const searchInput = document.getElementById('searchQuery');
            let text = searchInput.value;
            if (text.includes(" ")) {
                searchInput.value = text.substring(0, text.lastIndexOf(" ") + 1) + item;
            } else {
                searchInput.value = item;
            }
            autocompleteList.innerHTML = '';
            hideAutocomplete();
            searchInput.focus();
        };
        autocompleteList.appendChild(div);
    });
    autocompleteList.style.display = 'block';
    searchQuery.style.borderBottomLeftRadius = '0px';
    searchQuery.style.borderBottomRightRadius = '0px';
}

document.addEventListener('click', function(event) {
    const searchBox = document.getElementById('searchBox');
    if (!searchBox.contains(event.target)) {
        hideAutocomplete();
    }
});


/* search bar keyboard event listener */
document.getElementById('searchQuery').addEventListener('keydown', function(event) {
    const autocompleteList = document.getElementById('autocompleteList');
    let activeItem = autocompleteList.querySelector('.active');
    const items = autocompleteList.getElementsByTagName('div');

    switch (event.key) {
        case 'ArrowDown':
            event.preventDefault();
            if (!activeItem) {
                items[0].classList.add('active');
            } else if (activeItem.nextSibling === null) {
                activeItem.classList.remove('active');
                items[0].classList.add('active');
            } else {
                activeItem.classList.remove('active');
                activeItem.nextSibling.classList.add('active');
            }
            break;
        case 'ArrowUp':
            event.preventDefault();
            if (!activeItem || activeItem.previousSibling === null) {
                items[items.length - 1].classList.add('active');
            } else {
                activeItem.classList.remove('active');
                activeItem.previousSibling.classList.add('active');
            }
            break;
        case 'Enter':
            if (autocompleteList.style.display !== 'none' && activeItem) {
                event.preventDefault(); // Prevent form submission
                activeItem.click(); // Simulate click on the active item
            }
            break;
        case 'Escape':
            hideAutocomplete();
            break;
    }
});


function hideAutocomplete() {
    const autocompleteList = document.getElementById('autocompleteList');
    const searchQuery = document.getElementById('searchQuery');
    autocompleteList.style.display = 'none';
    searchQuery.style.borderBottomLeftRadius = '25px';
    searchQuery.style.borderBottomRightRadius = '25px';
}

function showLoadingWrapper() {
    const loadingWrapper = document.querySelector('.loading-wrapper')
    const loadingBox = document.querySelector('.loading-box')
    loadingWrapper.style.display = 'flex';
    loadingBox.style.display = 'grid';
    loadingBox.classList.add('rotate');
}

function hideLoadingWrapper() {
    const loadingWrapper = document.querySelector('.loading-wrapper')
    const loadingBox = document.querySelector('.loading-box')
    loadingWrapper.style.display = 'none';
    loadingBox.style.display = 'none';
    loadingBox.classList.remove('rotate');
}




