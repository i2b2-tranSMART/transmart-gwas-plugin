<!DOCTYPE html>
<html>
<head>
	%{--Force Internet Explorer 8 to override compatibility mode--}%
	<meta http-equiv="X-UA-Compatible" content="IE=edge">

	<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1"/>
	<title>${grailsApplication.config.com.recomdata.searchtool.appTitle}</title>

	%{--jQuery CSS for cupertino theme--}%
	<link rel="stylesheet" href="${resource(dir: 'css/jquery/cupertino', file: 'jquery-ui-1.8.18.custom.css')}"></link>
	<link rel="stylesheet" href="${resource(dir: 'css/jquery/skin', file: 'ui.dynatree.css')}"></link>

	%{--Our CSS--}%
	<link rel="stylesheet" href="${resource(dir: 'css', file: 'jquery.loadmask.css')}"></link>
	<link rel="stylesheet" href="${resource(dir: 'css', file: 'main.css')}"></link>
	<link rel="stylesheet" href="${resource(dir: 'css', file: 'colorbox.css')}"></link>
	<link rel="stylesheet" href="${resource(dir: 'css/jquery', file: 'simpleModal.css')}"></link>
	<link rel="stylesheet" href="${resource(dir: 'css/jquery/multiselect', file: 'ui.multiselect.css')}"></link>
	<link rel="stylesheet" href="${resource(dir: 'css/jquery/multiselect', file: 'common.css')}"></link>

	%{--jQuery JS libraries--}%
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery-1.8.3.min.js')}"></script>
	<script>jQuery.noConflict();</script>

	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery-ui.min.js')}"></script>

	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery.cookie.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery.dynatree.min.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery.paging.min.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery.loadmask.min.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery.ajaxmanager.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery.numeric.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery.colorbox-min.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery.simplemodal.min.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'jquery.dataTables.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/facetedSearch', file: 'facetedSearchBrowse.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/jQuery', file: 'ui.multiselect.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/help', file: 'D2H_ctxt.js')}"></script>

	<g:ifPlugin name='folder-management'>
	<g:render template='/folderManagementUrls' plugin='folderManagement'/>
	<script type="text/javascript" src="${resource(dir: 'js', file: 'folderManagement.js')}"></script>
	<link rel="stylesheet" href="${resource(dir: 'css', file: 'folderManagement.css')}"></link>
	</g:ifPlugin>

	<script type="text/javascript" src="${resource(dir: 'js', file: 'gwas.js')}"></script>
	<link rel="stylesheet" href="${resource(dir: 'css', file: 'gwas.css')}"></link>

	%{--SVG Export--}%
	<script type="text/javascript" src="${resource(dir: 'js/svgExport', file: 'rgbcolor.js')}"></script>

	<g:javascript library='prototype'/>

	%{--Our JS--}%

	<script type="text/javascript" src="${resource(dir: 'js', file: 'maintabpanel.js')}"></script>

	%{--Protovis Visualization library and IE plugin (for lack of SVG support in IE8)--}%
	<script type="text/javascript" src="${resource(dir: 'js/protovis', file: 'protovis-r3.2.js')}"></script>
	<script type="text/javascript" src="${resource(dir: 'js/protovis', file: 'protovis-msie.min.js')}"></script>

	<script>
		var searchResultsURL = "${createLink(action: 'loadSearchResults')}";
		var facetResultsURL = "${createLink(action: 'getFacetResults')}";
		var facetTableResultsURL = "${createLink(action: 'getFacetResultsForTable')}";
		var newSearchURL = "${createLink(action: 'newSearch')}";
		var visualizationURL = "${createLink(action: 'newVisualization')}";
		var tableURL = "${createLink(action: 'newTable')}";
		var treeURL = "${createLink(action: 'getDynatree')}";
		var sourceURL = "${createLink(action: 'searchAutoComplete')}";
		var getCategoriesURL = "${createLink(action: 'getSearchCategories')}";
		var getHeatmapNumberProbesURL = "${createLink(action: 'getHeatmapNumberProbes')}";
		var getHeatmapDataURL = "${createLink(action: 'getHeatmapData')}";
		var getHeatmapDataForExportURL = "${createLink(action: 'getHeatmapDataForExport2')}";
		var getBoxPlotDataURL = "${createLink(action: 'getBoxPlotData')}";
		var getLinePlotDataURL = "${createLink(action: 'getLinePlotData')}";
		var saveSearchURL = "${createLink(action: 'saveFacetedSearch')}";
		var loadSearchURL = "${createLink(action: 'loadFacetedSearch')}";
		var deleteSearchURL = "${createLink(action: 'deleteFacetedSearch')}";
		var exportAsImage = "${createLink(action: 'exportAsImage')}";

		var getStudyAnalysesUrl = "${createLink(controller: 'GWAS',action: 'getTrialAnalysis')}";

		//These are the URLS for the different browse windows.
		var studyBrowseWindow = "${createLink(controller: 'experiment', action: 'browseExperimentsMultiSelect')}";
		var analysisBrowseWindow = "${createLink(controller: 'experimentAnalysis', action: 'browseAnalysisMultiSelect')}";
		var dataTypeBrowseWindow = "${createLink(controller: 'GWAS', action: 'browseDataTypesMultiSelect')}";
	</script>

	<g:ifPlugin name='transmart-gwas'>
	<g:render template='/GWAS/gwasURLs'/>
	</g:ifPlugin>

	<script>
		var mouse_inside_options_div = false;
		var popupWindowPropertiesMap = [];

		jQuery(document).ready(function () {
			addSelectCategories();
			addSearchAutoComplete();
			addToggleButton();
			popupWindowPropertiesMap['Study'] = {
				'URLToUse': studyBrowseWindow,
				'filteringFunction': applyPopupFiltersStudy
			};
			popupWindowPropertiesMap['Analyses'] = {
				'URLToUse': analysisBrowseWindow,
				'filteringFunction': applyPopupFiltersAnalyses
			};
			popupWindowPropertiesMap['Data Type'] = {
				'URLToUse': dataTypeBrowseWindow,
				'filteringFunction': applyPopupFiltersDataTypes
			};

			jQuery("#xtButton").colorbox({opacity: .75, inline: true, width: "95%", height: "95%"});

			showSearchResults('analysis'); //reload the full search results for the analysis/study view

			//Disabling this, we aren't using the d3js code that takes advantage of HTML5.
			//showIEWarningMsg();

			jQuery("#searchResultOptions_btn").click(function () {
				jQuery("#searchResultOptions").toggle();
			});

			//used to hide the options div when the mouse is clicked outside of it

			jQuery('#searchResultOptions_holder').hover(function () {
				mouse_inside_options_div = true;
			}, function () {
				mouse_inside_options_div = false;
			});

			jQuery("body").mouseup(function () {
				//top menu options
				if (!mouse_inside_options_div) {
					jQuery('#searchResultOptions').hide();
				}

				var analysisID = jQuery('body').data('heatmapControlsID');

				if (analysisID > 1) {
					jQuery('#heatmapControls_' + analysisID).hide();
				}
			});

			jQuery('#topTabs').tabs();
			jQuery('#topTabs').bind("tabsshow", function (event, ui) {
				var id = ui.panel.id;
				if (ui.panel.id === "results-div") {
				}
				else if (ui.panel.id === "table-results-div") {
				}
			});
		});

		jQuery(function ($) {
			// Load dialog on click of Save link
			$('#save-modal .basic').click(openSaveSearchDialog);
		});
	</script>

	<r:layoutResources/><%-- XXX: Use template --%>
</head>
<body>
<div id="header-div">
	<g:render template='/layouts/commonheader' model="[app: 'gwas', utilitiesMenu: 'true']"/>
</div>

<div id="main">
	<div id="menu_bar">
		<div class='toolbar-item' onclick='collapseAllStudies();'>Collapse All Studies</div>

		<div class='toolbar-item' onclick='expandAllStudies();'>Expand All Studies</div>
		<g:ifPlugin name='transmart-gwas'>
		<div class='toolbar-item' onclick='openPlotOptions();'>Manhattan Plot</div>
		</g:ifPlugin>
		<div class='toolbar-item' onclick="jQuery('.analysesopen .analysischeckbox').attr('checked', 'checked'); updateSelectedAnalyses();">
		Select All Visible Analyses
		</div>

		<div class='toolbar-item' onclick="jQuery('.analysesopen .analysischeckbox').removeAttr('checked'); updateSelectedAnalyses();">
		Unselect All Visible Analyses
		</div>

		<div class='toolbar-item' onclick="filterSelectedAnalyses();">Add Selected to Filter</div>

		<g:ifPlugin name='folder-management'>
		<div class="toolbar-item">
			<g:render template='/fmFolder/exportCart' model="[exportCount: exportCount]" plugin='folderManagement'/>
		</div>
		</g:ifPlugin>

		<div style="display: inline-block;">
			<tmpl:/GWAS/helpIcon id="1289"/>
		</div>
	</div>

	<div id="topTabs" class="analysis-tabs">
		<ul>
			<li id="analysisViewTab"><a href="#results-div" onclick="showSearchResults('analysis')">Analysis View</a></li>
			<li id="tableViewTab"><a href="#table-results-div" onclick="showSearchResults('table')">Table View</a></li>
		</ul>

		<div id="results-div">
		</div>

		<div id="table-results-div">
			<div id="results_table_wrapper" class="dataTables_wrapper" role="grid">
			</div>
		</div>
	</div>
</div>

<div id="search-div">
	<select id="search-categories"></select>
	<input id="search-ac"/></input>
</div>

<div id="title-search-div" class="ui-widget-header boxtitle">
	<h2 style="float:left" class="title">Active Filters</h2>
	<div id="activeFilterHelp" style="float: right; margin: 2px">
		<tmpl:/GWAS/helpIcon id="1298"/>
	</div>
	<h2 style="float:right; padding-right:5px;" class="title">
		<%-- Save/load disabled for now
		<span id='save-modal'>
			<a href="#" class="basic">Save</a>
		</span>
		<a href="#" onclick="loadSearch(); return false;">Load</a>--%>
		<a href="#" onclick="clearSearch(); return false;">Clear</a>
	</h2>
</div>

%{--Save search modal content--}%
<div id="save-modal-content" style="display:none;">
	<h3>Save Faceted Search</h3><br/>
	Enter Name <input type="text" id="searchName" size="50"/><br/><br/>
	Enter Description <textarea id="searchDescription" rows="5" cols="70"></textarea><br/>
	<br/>
	<a href="#" onclick="saveSearch(); return false;">Save</a>&nbsp;
	<a href="#" onclick="jQuery.modal.close(); return false;">Cancel</a>
</div>

<div id="active-search-div"></div>

<div id="title-filter" class="ui-widget-header boxtitle">
	<h2 style="float:left" class="title">Filter Browser</h2>
	<div id="filterBrowserHelp" style="float: right; margin: 2px">
		<tmpl:/GWAS/helpIcon id="1297"/>
	</div>
</div>
<div id="side-scroll">
	<div id="filter-div"></div>
</div>
<button id="toggle-btn"></button>
<div id="searchHelp" style="position: absolute; top: 30px; left: 272px">
	<tmpl:/GWAS/helpIcon id="1296"/>
</div>

<div id="hiddenItems" style="display:none">
	%{--For image export--}%
	<canvas id="canvas" width="1000px" height="600px"></canvas>
</div>

%{--This is the DIV we stuff the browse windows into.--}%
<div id="divBrowsePopups" style="width:800px; display: none;">
</div>

<g:ifPlugin name='transmart-gwas'>
<g:render template='/manhattan/plotOptions'/>
</g:ifPlugin>
%{--This DIV for export Analysis details--}%
<div id="divMailStatus"></div>
<div id="divTomailIds" style="width:300px; display: none;">
	<table class="columndetail">
		<tr>
			<td class="columnname">Send mail to :</td>
			<td><input id="toEmailID" style="width: 210px"></td>
		</tr>
	</table>
	<br><br>
	<g:radio name='radioMail' value='link' checked='true'/>Send as Link (full set) <br><br>
	<g:radio name='radioMail' value='attachment'/>Send as Attachment(top 200 rows)
</div>
%{--Everything for the across trial function goes here and is displayed using colorbox--}%
<div style="display:none">
	<div id="xtHolder">
		<div id="xtTopbar">
			<p>Cross Trial Analysis</p>
			<ul id="xtMenu">
				<li>Summary</li>
				<li>Heatmap</li>
				<li>Boxplot</li>
			</ul>
			<p>close</p>
		</div>

		<div id="xtSummary">%{--Summary Tab Content--}%
		</div>

		<div id="xtHeatmap">%{--Heatmap Tab Content--}%
		</div>

		<div id="xtBoxplot">%{--Boxplot Tab Content--}%
		</div>
	</div>
</div>
%{--Used to measure the width of a text element (in svg plots)--}%
<span id="ruler" style="visibility: hidden; white-space: nowrap;"></span>

<div id="analysisViewHelp" style="position: absolute; top: 70px; right: 20px; display: none;">
	<tmpl:/GWAS/helpIcon id="1358"/>
</div>
<div id="tableViewHelp" style="position: absolute; top: 70px; right: 20px; display: none;">
	<tmpl:/GWAS/helpIcon id="1317"/>
</div>

<g:ifPlugin name="folder-management">
<div id="exportOverlay" class="overlay" style="display: none;">&nbsp;</div>
</g:ifPlugin>
<r:layoutResources/><%-- XXX: Use template --%>
</body>
</html>
