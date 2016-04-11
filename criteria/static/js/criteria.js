(function( criteria, $, undefined ) {
	
	// get criteria details for criteria section
	criteria.get_criteria_details = function(feature_id, app_name) {
	
		url_ = "/" + app_name + "/criteria/";
		$.ajax({
			type: "POST",
			url: url_,
			data: {'feature_id': feature_id},
		    beforeSend: function(xhr, settings) {
		        if (!this.crossDomain) {
		            xhr.setRequestHeader("X-CSRFToken", pydgin_utils.getCookie('csrftoken'));
		        }
		    },
			success: function(hits, textStatus, jqXHR) {
			
				feature_id_ori = feature_id
				feature_id = feature_id.replace(/\./g, '_');
										
				pydgin_utils.add_spinner_before('table-criteria-'+feature_id, "criteria-spinner-"+feature_id);
				
				
				if(hits.hits.length == 0){
					row = "<p style='padding:10px'>No results found</p>"
					$('#criteria-'+feature_id).replaceWith(row);
				}

				var detail_row = "";
				
				for(var i=0; i<hits.hits.length; i++) {
					var idx =  hits.hits[i]['_index'];
					var type = hits.hits[i]['_type'];
					var meta_info = hits['meta_info'];
					var link_info = hits['link_info'];
					var agg_disease_tags = hits['agg_disease_tags'].sort()

					criteria_desc = meta_info[idx][type];
										
					link_id_type = link_info[idx][type];
					
        			var hit = hits.hits[i]._source;
					var disease_tags = hit.disease_tags.sort()
			
				
					
					var features_list = {}
					
					$.each(disease_tags, function( index, dis_code ) {
						notes_list = hits.hits[i]['_source'][dis_code]
						
						$.each(notes_list, function( index, notes_dict ) {
							var current_row = "";
							current_row += '<a href="/' + link_id_type +'/' + notes_dict['fid'] + '/">';
							current_row += notes_dict['fname'] ;
							current_row += '</a>';
							
							if(current_row in features_list){
								cur_dis_list = features_list[current_row];
								cur_dis_list.push(dis_code);
								features_list[current_row] = cur_dis_list;
							}else{
								var dis_list = []
								dis_list.push(dis_code);
								features_list[current_row] = dis_list;
							}
							

						});
						
					});
					
					var lc_desc = type.toLowerCase();
					var lc_desc_ = lc_desc.replace(/\s+/g,"_");
			
					if(Object.keys(features_list).length == 1){
						for (var firstKey in features_list) break;
						$('div[id="'+lc_desc_+'"]').append(firstKey)
					}else{
						console.log(lc_desc_)
						var show_button = '<button class="btn btn-sm btn-default" id="criteria_details_button_'+ lc_desc_ +'" data-toggle="collapse" data-target="#criteria_details_'+lc_desc_ +'">DETAILS</button>'
						$('div[id="'+lc_desc_+'"]').append(show_button)
						    detail_row = "";
							detail_row += '<table  class="table-striped table-bordered" >';  
							detail_row += "<tr>";
						$.each( features_list, function( feature, dis_codes ) {

							detail_row += "<td width='150px'>";
							detail_row += feature;
							detail_row += "</td>";

							detail_row += '<td>';
							detail_row += '<div class="disease-bar">';
							$.each(dis_codes, function( index, dis_code ) {
							 detail_row += '<a class="btn btn-default btn-disease ' + dis_code + '">' + dis_code + '</a>';
							});
							detail_row += '</div>';
							detail_row += '</td>';
							detail_row += '</tr>';
						 
							
						});
						detail_row += '</table	>';  
					
												
						detail_row2 = "";
						
						//detail_row2 += "<div id='criteria_details_"+ lc_desc_  +"'  class='collapse col-md-9 col-md-offset-3' style='background-color:#f8f5f0' >";
						detail_row2 += "<div id='criteria_details_"+ lc_desc_  +"'  class='collapse col-md-9 col-md-offset-3' >";
						detail_row2 += detail_row;
						detail_row2 += '</div>';
						$('div[id="criteria_row_'+ lc_desc_+'"]' ).after(detail_row2);
					

					}
					
        								
				}
	
				$("#criteria-spinner-"+feature_id).remove();
	
				
			}
		});
	}
	
}( window.criteria = window.criteria || {}, jQuery ));









