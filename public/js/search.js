var lastFocusedElement;

$(document).ready(function() {
    $('#btn-search')[0].addEventListener('click', function() { openSearch(false); });
    $('#btn-search-close')[0].addEventListener('click', closeSearch);
    document.addEventListener('keydown', function(ev) {
        if( ev.keyCode == 27 ) {
            closeSearch();
        }

        if (ev.srcElement.localName === 'body' &&
            ((event.keyCode >= 48 && event.keyCode <= 57) || (event.keyCode >= 65 && event.keyCode <= 90))) {
                openSearch(true);
        }
    });
});

function openSearch(autocomplete) {
    //$('#search__inner--down')[0].scrollTo(0,0);
    lastFocusedElement = document.activeElement;
    $('#search')[0].classList.add('search--open');
    if (autocomplete) {
        $('#search__input')[0].focus();
    }else{
        setTimeout(function() {
            $('#search__input')[0].focus();
        }, 500);
    }
}

function closeSearch() {
    console.log("Closing search ..");
    var inputSearch = $('#search__input')[0];
    $('#search')[0].classList.remove('search--open');
    inputSearch.blur();
    inputSearch.value = '';
    if (lastFocusedElement) {
        lastFocusedElement.focus();
    }
}

