const state = {
    section: "main"
};

const setSection = (section) => {
    state.section = section
};

const fetchParagraphSmokingSentences = async (paragraphMetaData) => {
    const {paperId, paragraphIndex} = paragraphMetaData;
    const url = new URL('http://localhost:9000/smoking/api/sentences/paragraph');
    const params = {paperId: paperId, paragraphIndex: paragraphIndex};
    url.search = new URLSearchParams(params).toString();
    const response = await fetch(url.href);
    return response.json();
};

const fetchSmokingSentenceById = async (similarSentence) => {
    const {paperId, paragraphIndex, sentenceIndex} = similarSentence;
    const url = new URL('http://localhost:9000/smoking/api/sentence');
    const params = {paperId: paperId, paragraphIndex: paragraphIndex, sentenceIndex: sentenceIndex};
    url.search = new URLSearchParams(params).toString();
    const response = await fetch(url.href);
    return response.json();
}

const fetchSmokingParagraphById = async (paragraphMetaData) => {
    const {paperId, paragraphIndex} = paragraphMetaData;
    const url = new URL('http://localhost:9000/smoking/api/paragraph');
    const params = {paperId: paperId, paragraphIndex: paragraphIndex};
    url.search = new URLSearchParams(params).toString();
    const response = await fetch(url.href);
    return response.json();
}

const fetchAllSmokingSentences = async () => {
    const response = await fetch('http://localhost:9000/smoking/api/sentences');
    return response.json();
};

const fetchAllSmokingParagraphs = async () => {
    const response = await fetch('http://localhost:9000/smoking/api/paragraphs');
    return response.json();
};

const findSectionByUrlPath = () => {
    const pathParts = window.location.pathname.split("/");
    const lastPart = pathParts[pathParts.length - 1];
    if (lastPart === "paragraphs") setSection("paragraphs");
    else if (lastPart === "sentences") setSection("sentences");
    else setSection("main")
};

const exploreSentences = (paragraphMetaData) => {
    buildParagraphSentencesView(paragraphMetaData);
    const sentencesTabSelector = document.getElementById("sentencesTabSelector");
    sentencesTabSelector.click();
};

const focusParagraph = (paragraphMetaData) => {
    buildFocusParagraphView(paragraphMetaData);
    const paragraphTabSelector = document.getElementById("paragraphsTabSelector");
    paragraphTabSelector.click();
}

const addParagraphsToView = (paragraphs) => {
    paragraphs.forEach(paragraphData => createParagraphCard(paragraphData));
}

const sortWordsByTextCoordinateIndex = (indicatorA, indicatorB) => {
    const {begin: beginA} = indicatorA;
    const {begin: beginB} = indicatorB;
    return beginB - beginA
};

const chopParagraphIntoSentences = (text, indicators) => {
    // We cop of peaces from the text starting at the end (in order not to mess up indices).
    // Therefore we need to order the indicators by descending text coordinate index.
    const sortedIndicators = indicators.sort(sortWordsByTextCoordinateIndex);
    // The rest of the text after each chopping step is stored here.
    let textRest = text;
    // This dictionary stores the peaces we have chopped of together with a tag.
    let textSnippets = [];
    for (let i = 0; i < sortedIndicators.length; i++) {
        const {sentenceBegin, sentenceEnd} = sortedIndicators[i];
        // Chop of the text behind the indicator if there is any.
        if (sentenceEnd < textRest.length - 1) {
            const textAfterIndicator = textRest.substring(sentenceEnd + 1);
            textSnippets.push([textAfterIndicator, "plain"]);
            textRest = textRest.substring(0, sentenceEnd + 1);
        }
        // Chop of indicator. However make sure that it is still within the text.
        if (sentenceBegin < textRest.length) {
            const textIndicator = textRest.substring(sentenceBegin);
            textSnippets.push([textIndicator, "tagged"]);
            textRest = textRest.substring(0, sentenceBegin);
        }
    }
    // If there is still text left before the indicators, we need to add it at the end.
    if (textRest.length > 0) textSnippets.push([textRest, "plain"]);
    return textSnippets.reverse();
};

const chopSentencesIntoPos = (text, indicators, pos) => {
    // We cop of peaces from the text starting at the end (in order not to mess up indices).
    // Therefore we need to order the POS by descending text coordinate index.
    const sortedPos = pos.sort(sortWordsByTextCoordinateIndex);
    // The rest of the text after each chopping step is stored here.
    let textRest = text;
    // This dictionary stores the peaces we have chopped of together with a tag.
    let textSnippets = [];
    for (let i = 0; i < sortedPos.length; i++) {
        const {begin, end, tag} = sortedPos[i];
        // Chop of the text behind the POS if there is any.
        if (end < textRest.length - 1) {
            const textAfterPos = textRest.substring(end + 1);
            textSnippets.push([textAfterPos, "plain"]);
            textRest = textRest.substring(0, end + 1);
        }
        // Chop of POS. However make sure that it is still within the text.
        if (begin < textRest.length) {
            const textPos = textRest.substring(begin);
            textSnippets.push([textPos, mapPosIntoCategories(tag)]);
            textRest = textRest.substring(0, begin);
        }
    }
    // If there is still text left before the POS, we need to add it at the end.
    if (textRest.length > 0) textSnippets.push([textRest, "plain"]);
    return textSnippets.reverse();
};

const mapPosIntoCategories = (posTag) => {
    if (posTag.startsWith("N")) {
        return 'noun';
    } else if (posTag.startsWith("J")) {
        return 'adjective';
    } else if (posTag.startsWith("V")) {
        return 'verb';
    } else {
        return 'plain'
    }
}

const buildHighlightedText = (textSnippets) => {
    const indicatorTagOpen = '<span style="background-color: darksalmon; color: white;">';
    const nounTagOpen = '<span class="defaultStyleForText nounTextColor" style="background-color: palevioletred; color: white;">';
    const adjectiveTagOpen = '<span class="defaultStyleForText adjectiveTextColor" style="background-color: darkseagreen; color: white;">';
    const verbTagOpen = '<span class="defaultStyleForText verbTextColor" style="background-color: cornflowerblue; color: white;">';
    const tagClose = '</span>';

    let html = "";
    textSnippets.forEach(snippet => {
        switch (snippet[1]) {
            case 'plain':
                html = html + snippet[0];
                break;
            case 'tagged':
                html = html + indicatorTagOpen + snippet[0] + tagClose;
                break;
            case 'noun':
                html = html + nounTagOpen + snippet[0] + tagClose;
                break;
            case 'verb':
                html = html + verbTagOpen + snippet[0] + tagClose;
                break;
            case 'adjective':
                html = html + adjectiveTagOpen + snippet[0] + tagClose;
                break;
        }
    });
    return html;
};

const createParagraphCard = (paragraphData) => {
    const {paperId, paragraphIndex, text, indicators} = paragraphData;
    const textSnippets = chopParagraphIntoSentences(text, indicators);
    textSnippets.forEach(sn => console.log(sn[1] + "->" + sn[0]));
    const paragraphTemplate = document.getElementById("paragraphCardTemplate").content;
    const paragraphCard = paragraphTemplate.cloneNode(true);
    const textBlock = paragraphCard.getElementById("textBlock");
    textBlock.innerHTML = buildHighlightedText(textSnippets);
    const sentencesBtn = paragraphCard.getElementById("sentencesBtn");
    const paragraphMetaData = {
        paperId: paperId,
        paragraphIndex: paragraphIndex,
        sentenceIndices: extractIndicatorSentenceIndices(indicators)
    };
    sentencesBtn.onclick = (event) => exploreSentences(paragraphMetaData);
    document.getElementById("paragraphsTab").appendChild(paragraphCard)
};

const extractIndicatorSentenceIndices = (paragraphIndicators) => {
    return paragraphIndicators.map(indicator => {
        const {sentenceIndex} = indicator;
        return sentenceIndex
    })
};

const clearChildrenFromNode = (elementId) => {
    const domNode = document.getElementById(elementId);
    while (domNode.firstChild) domNode.removeChild(domNode.lastChild)
};

const addSentencesToView = (domNode, sentences) => {
    sentences.forEach(sentenceData => createSentenceCard(domNode, sentenceData))
};

const createSentenceCard = (domNode, sentenceData, similarClassHighlighting = false, similarSentence) => {
    const isMainTab = domNode.id === 'sentencesTab'
    const {text, indicators, pos, similar, paperId, paragraphIndex} = sentenceData;
    const textSnippets = chopSentencesIntoPos(text, indicators, pos);
    const sentenceTemplate = document.getElementById("sentenceCardTemplate").content;
    const sentenceCard = sentenceTemplate.cloneNode(true);
    const textBlock = sentenceCard.getElementById("textBlock");
    const indicatorOpenTag = "<b>"
    const indicatorCloseTag = "</b>"

    if (similarClassHighlighting) {
        textBlock.parentElement.classList.add('similarBlockColor');
    }

    textBlock.innerHTML = buildHighlightedText(textSnippets);
    indicators.forEach(element => {
        textBlock.innerHTML = textBlock.innerHTML.replace(element.word, indicatorOpenTag + element.word + indicatorCloseTag)
    });

    const similarBlock = sentenceCard.getElementById("similarBlock");
    const nounButton = sentenceCard.getElementById("nounButton");
    const verbButton = sentenceCard.getElementById("verbButton");
    const adjectiveButton = sentenceCard.getElementById("adjectiveButton");

    buildButtonCallbacksForTextHighlighting(nounButton, verbButton, adjectiveButton, textBlock);

    const focusParagraphBtn = sentenceCard.getElementById("focusParagraphBtn");
    focusParagraphBtn.onclick = (event) => focusParagraph({
        paperId: paperId,
        paragraphIndex: paragraphIndex
    });

    const similarButton = sentenceCard.getElementById('similarSwitch');

    const nearSimilar = similar.filter(similarSentence => {
        const {distance} = similarSentence;
        return distance < 0.8 && distance > 0
    });
    if (nearSimilar.length > 0 && isMainTab) {
        similarButton.onclick = (event) => showSimilarData(event, similarBlock, nearSimilar);
    } else {
        similarButton.parentElement.parentElement.style.display = 'none';
        const messageTag = document.createElement("p");
        if (!similarClassHighlighting) {
            messageTag.innerText = "No similar texts found";
        }
        else {
            const similarity = ((1 - similarSentence.distance) * 100).toFixed(2);
            messageTag.innerHTML = `<progress max="100" value="${similarity}"></progress><span style="margin-left: 20px">${similarity}% similarity</span>`
        }
        similarBlock.appendChild(messageTag);
    }
    domNode.appendChild(sentenceCard)
};

const addSimilarSwitch = (domNode, callbackOnClick) => {
    domNode.onclick = (event) => callbackOnClick(event);
    domNode.appendChild(switchButton);
};

const showSimilarData = (event, similarBlock, similar) => {
    if (event.target.checked) {
        similar.forEach(similarSentence => {
            fetchSmokingSentenceById(similarSentence).then(sentenceJson => {
                createSentenceCard(similarBlock, sentenceJson, true, similarSentence);
            });
        });
    } else {
        event.target.parentElement.classList.remove('is-checked');
        while (similarBlock.firstChild) similarBlock.removeChild(similarBlock.lastChild)
    }
}

const buildParagraphSentencesView = (paragraphMetaData) => {
    const sentencesTabId = "sentencesTab"
    fetchParagraphSmokingSentences(paragraphMetaData).then(sentencesJson => {
        clearChildrenFromNode(sentencesTabId);
        addSentencesToView(document.getElementById(sentencesTabId), sentencesJson);
    });
};

const buildFocusParagraphView = (paragraphMetaData) => {
    fetchSmokingParagraphById(paragraphMetaData).then(paragraphJson => {
        clearChildrenFromNode('paragraphsTab');
        addParagraphsToView([paragraphJson])
    });
}

const buildAllSentencesView = () => {
    fetchAllSmokingSentences().then(sentencesJson => addSentencesToView(document.getElementById("sentencesTab"), sentencesJson));
};

const buildAllParagraphsView = () => {
    fetchAllSmokingParagraphs().then(paragraphsJson => addParagraphsToView(paragraphsJson))
};

const buildInitialView = () => {
    buildAllParagraphsView();
    buildAllSentencesView();
};

const buildButtonCallbacksForTextHighlighting = (nounButton, verbButton, adjectiveButton, textBlock) => {
    const buttonArray = [
        [nounButton, '.nounTextColor'],
        [verbButton, '.verbTextColor'],
        [adjectiveButton, '.adjectiveTextColor']
    ];
    buttonArray.forEach(button => {
        button[0].onclick = (event) => {
            const value = textBlock.querySelectorAll(button[1]);
            if (value.length > 0) {
                const isDefaultStyle = value[0].classList.contains("defaultStyleForText");
                value.forEach(element => {
                    if (isDefaultStyle) {
                        element.classList.remove("defaultStyleForText")
                    } else {
                        element.classList.add("defaultStyleForText")
                    }
                })
            }
        }
    })
}

findSectionByUrlPath();
buildInitialView();