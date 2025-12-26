"""
Модуль для экспорта данных в различные форматы.
Содержит эндпоинты для экспорта отчетов в CSV, Excel и PDF.
"""
from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import StreamingResponse, JSONResponse
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd
import io
import os
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
import matplotlib.pyplot as plt
import seaborn as sns
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import json

from app.models import (ErrorResponse, ValidationErrorResponse)
from app.database import get_report_by_id, store_export
from app.report_generator import (
    generate_zone_occupancy_report,
    generate_time_in_zone_report,
    generate_workflow_efficiency_report
)

router = APIRouter(tags=["Export"])
logger = logging.getLogger(__name__)

@router.get(
    "/export/csv",
    responses={
        200: {"description": "Файл CSV для скачивания"},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def export_to_csv_endpoint(
    report_type: str = Query(..., description="Тип отчета", enum=["zone_occupancy", "time_in_zone", "workflow_efficiency", "anomalies"]),
    start_time: str = Query(..., description="Начало периода"),
    end_time: str = Query(..., description="Конец периода"),
    zone_ids: Optional[str] = Query(None, description="Список ID зон через запятую"),
    entity_types: Optional[str] = Query(None, description="Список типов сущностей через запятую")
):
    """
    Экспорт данных в CSV.
    Экспорт отчета в формате CSV.
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        zone_id_list = zone_ids.split(',') if zone_ids else None
        entity_type_list = entity_types.split(',') if entity_types else None
        
        # Генерируем отчет в зависимости от типа
        if report_type == "zone_occupancy":
            report = generate_zone_occupancy_report(start_dt, end_dt, zone_id_list, entity_type_list)
            df = _convert_zone_occupancy_to_dataframe(report)
        elif report_type == "time_in_zone":
            report = generate_time_in_zone_report(None, None, start_dt, end_dt, "day")
            df = _convert_time_in_zone_to_dataframe(report)
        elif report_type == "workflow_efficiency":
            report = generate_workflow_efficiency_report(start_dt, end_dt, zone_id_list, None)
            df = _convert_workflow_efficiency_to_dataframe(report)
        else:  # anomalies
            # Здесь будет генерация отчета об аномалиях
            report = {}
            df = pd.DataFrame()
        
        # Преобразуем в CSV
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        # Сохраняем информацию об экспорте
        export_id = _save_export_info(report_type, start_dt, end_dt, "csv", len(output.getvalue()))
        
        headers = {
            "Content-Disposition": f'attachment; filename="report_{report_type}_{start_dt.strftime("%Y%m%d")}_{end_dt.strftime("%Y%m%d")}.csv"',
            "Content-Type": "text/csv"
        }
        
        return StreamingResponse(iter([output.getvalue()]), headers=headers)
        
    except ValueError as e:
        logger.error(f"Invalid datetime format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="INVALID_DATETIME_FORMAT",
                message=f"Invalid datetime format: {str(e)}"
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="EXPORT_ERROR",
                message=f"Failed to export to CSV: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/export/excel",
    responses={
        200: {"description": "Файл Excel для скачивания"},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def export_to_excel_endpoint(
    report_type: str = Query(..., description="Тип отчета", enum=["zone_occupancy", "time_in_zone", "workflow_efficiency", "anomalies"]),
    start_time: str = Query(..., description="Начало периода"),
    end_time: str = Query(..., description="Конец периода"),
    zone_ids: Optional[str] = Query(None, description="Список ID зон через запятую"),
    entity_types: Optional[str] = Query(None, description="Список типов сущностей через запятую"),
    include_charts: bool = Query(True, description="Включить диаграммы в отчет")
):
    """
    Экспорт данных в Excel.
    Экспорт отчета в формате Excel с возможностью включения диаграмм.
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        zone_id_list = zone_ids.split(',') if zone_ids else None
        entity_type_list = entity_types.split(',') if entity_types else None
        
        # Генерируем отчет
        if report_type == "zone_occupancy":
            report = generate_zone_occupancy_report(start_dt, end_dt, zone_id_list, entity_type_list)
            df = _convert_zone_occupancy_to_dataframe(report)
        elif report_type == "time_in_zone":
            report = generate_time_in_zone_report(None, None, start_dt, end_dt, "day")
            df = _convert_time_in_zone_to_dataframe(report)
        elif report_type == "workflow_efficiency":
            report = generate_workflow_efficiency_report(start_dt, end_dt, zone_id_list, None)
            df = _convert_workflow_efficiency_to_dataframe(report)
        else:  # anomalies
            # Здесь будет генерация отчета об аномалиях
            report = {}
            df = pd.DataFrame()
        
        # Создаем Excel файл
        output = io.BytesIO()
        
        # Если нужен файл с диаграммами, используем openpyxl
        if include_charts:
            # Сохраняем как Excel без диаграмм
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Data', index=False)
                
                # Добавляем сводные данные
                if report_type == "zone_occupancy" and hasattr(report, 'zones'):
                    summary_df = pd.DataFrame([{
                        'Zone Name': zone['zone_name'],
                        'Total Visits': zone['total_visits'],
                        'Unique Entities': zone['unique_entities'],
                        'Avg Duration (min)': zone['avg_duration_minutes']
                    } for zone in report.zones])
                    summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        else:
            # Простой экспорт без диаграмм
            df.to_excel(output, index=False)
        
        output.seek(0)
        
        # Сохраняем информацию об экспорте
        export_id = _save_export_info(report_type, start_dt, end_dt, "excel", len(output.getvalue()))
        
        headers = {
            "Content-Disposition": f'attachment; filename="report_{report_type}_{start_dt.strftime("%Y%m%d")}_{end_dt.strftime("%Y%m%d")}.xlsx"',
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        
        return StreamingResponse(iter([output.getvalue()]), headers=headers)
        
    except ValueError as e:
        logger.error(f"Invalid datetime format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="INVALID_DATETIME_FORMAT",
                message=f"Invalid datetime format: {str(e)}"
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error exporting to Excel: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="EXPORT_ERROR",
                message=f"Failed to export to Excel: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/export/pdf",
    responses={
        200: {"description": "Файл PDF для скачивания"},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def export_to_pdf_endpoint(
    report_type: str = Query(..., description="Тип отчета", enum=["zone_occupancy", "time_in_zone", "workflow_efficiency", "anomalies"]),
    start_time: str = Query(..., description="Начало периода"),
    end_time: str = Query(..., description="Конец периода"),
    zone_ids: Optional[str] = Query(None, description="Список ID зон через запятую"),
    entity_types: Optional[str] = Query(None, description="Список типов сущностей через запятую"),
    include_charts: bool = Query(True, description="Включить диаграммы в отчет")
):
    """
    Экспорт данных в PDF.
    Экспорт отчета в формате PDF с визуализацией.
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        zone_id_list = zone_ids.split(',') if zone_ids else None
        entity_type_list = entity_types.split(',') if entity_types else None
        
        # Генерируем отчет
        if report_type == "zone_occupancy":
            report = generate_zone_occupancy_report(start_dt, end_dt, zone_id_list, entity_type_list)
        elif report_type == "time_in_zone":
            report = generate_time_in_zone_report(None, None, start_dt, end_dt, "day")
        elif report_type == "workflow_efficiency":
            report = generate_workflow_efficiency_report(start_dt, end_dt, zone_id_list, None)
        else:  # anomalies
            # Здесь будет генерация отчета об аномалиях
            report = {}
        
        # Создаем PDF
        output = io.BytesIO()
        
        # Настраиваем стили
        styles = getSampleStyleSheet()
        title_style = styles['Title']
        heading_style = styles['Heading1']
        normal_style = styles['Normal']
        
        # Создаем документ
        doc = SimpleDocTemplate(output, pagesize=landscape(letter))
        story = []
        
        # Заголовок отчета
        title = f"{report_type.replace('_', ' ').title()} Report"
        story.append(Paragraph(title, title_style))
        story.append(Spacer(1, 12))
        
        # Период отчета
        period = f"Period: {start_dt.strftime('%Y-%m-%d %H:%M')} to {end_dt.strftime('%Y-%m-%d %H:%M')}"
        story.append(Paragraph(period, normal_style))
        story.append(Spacer(1, 24))
        
        # Данные отчета
        if report_type == "zone_occupancy" and hasattr(report, 'zones'):
            story.append(Paragraph("Zone Occupancy Summary", heading_style))
            story.append(Spacer(1, 12))
            
            # Создаем таблицу данных
            data = [["Zone Name", "Total Visits", "Unique Entities", "Avg Duration (min)"]]
            for zone in report.zones:
                data.append([
                    zone['zone_name'],
                    str(zone['total_visits']),
                    str(zone['unique_entities']),
                    str(zone['avg_duration_minutes'])
                ])
            
            table = Table(data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 14),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(table)
        
        # Если нужно включить диаграммы
        if include_charts and report_type == "zone_occupancy" and hasattr(report, 'zones'):
            story.append(Spacer(1, 24))
            story.append(Paragraph("Visual Charts", heading_style))
            story.append(Spacer(1, 12))
            
            # Создаем диаграмму в памяти
            fig, ax = plt.subplots(figsize=(10, 6))
            
            zone_names = [zone['zone_name'] for zone in report.zones]
            visit_counts = [zone['total_visits'] for zone in report.zones]
            
            ax.bar(zone_names, visit_counts)
            ax.set_title('Total Visits by Zone')
            ax.set_xlabel('Zone')
            ax.set_ylabel('Number of Visits')
            ax.tick_params(axis='x', rotation=45)
            
            # Сохраняем диаграмму во временный файл
            plt.tight_layout()
            
            # Конвертируем в изображение для PDF (это упрощенный пример)
            # В реальной реализации потребуется дополнительная обработка для вставки изображений в PDF
            logger.info("Chart generation for PDF requires additional implementation")
        
        # Собираем документ
        doc.build(story)
        output.seek(0)
        
        # Сохраняем информацию об экспорте
        export_id = _save_export_info(report_type, start_dt, end_dt, "pdf", len(output.getvalue()))
        
        headers = {
            "Content-Disposition": f'attachment; filename="report_{report_type}_{start_dt.strftime("%Y%m%d")}_{end_dt.strftime("%Y%m%d")}.pdf"',
            "Content-Type": "application/pdf"
        }
        
        return StreamingResponse(iter([output.getvalue()]), headers=headers)
        
    except ValueError as e:
        logger.error(f"Invalid datetime format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="INVALID_DATETIME_FORMAT",
                message=f"Invalid datetime format: {str(e)}"
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error exporting to PDF: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="EXPORT_ERROR",
                message=f"Failed to export to PDF: {str(e)}"
            ).model_dump()
        )

def _convert_zone_occupancy_to_dataframe(report: Any) -> pd.DataFrame:
    """Конвертирует отчет о посещаемости зон в DataFrame"""
    if not hasattr(report, 'zones') or not report.zones:
        return pd.DataFrame()
    
    data = []
    for zone in report.zones:
        hourly_distribution = zone.get('hourly_distribution', {})
        for hour, count in hourly_distribution.items():
            data.append({
                'zone_id': zone['zone_id'],
                'zone_name': zone['zone_name'],
                'zone_type': zone['zone_type'],
                'hour': hour,
                'visit_count': count,
                'total_visits': zone['total_visits'],
                'unique_entities': zone['unique_entities'],
                'avg_duration_minutes': zone['avg_duration_minutes'],
                'peak_hour': zone.get('peak_hour'),
                'entity_type_employees': zone['entity_breakdown'].get('employees', 0),
                'entity_type_equipment': zone['entity_breakdown'].get('equipment', 0)
            })
    
    return pd.DataFrame(data)

def _convert_time_in_zone_to_dataframe(report: Any) -> pd.DataFrame:
    """Конвертирует отчет о времени пребывания в зонах в DataFrame"""
    if not hasattr(report, 'data') or not report.data:
        return pd.DataFrame()
    
    data = []
    for record in report.data:
        data.append({
            'entity_id': record['entity_id'],
            'entity_name': record['entity_name'],
            'entity_type': record['entity_type'],
            'zone_id': record['zone_id'],
            'zone_name': record['zone_name'],
            'total_time_minutes': record['total_time_minutes'],
            'visits_count': record['visits_count'],
            'avg_visit_duration': record['avg_visit_duration'],
            'first_entry': record['first_entry'],
            'last_exit': record['last_exit']
        })
    
    return pd.DataFrame(data)

def _convert_workflow_efficiency_to_dataframe(report: Any) -> pd.DataFrame:
    """Конвертирует отчет об эффективности рабочих зон в DataFrame"""
    if not hasattr(report, 'zones') or not report.zones:
        return pd.DataFrame()
    
    data = []
    for zone in report.zones:
        workflow_metrics = zone.get('workflow_metrics', {})
        data.append({
            'zone_id': zone['zone_id'],
            'zone_name': zone['zone_name'],
            'utilization_rate': zone['utilization_rate'],
            'avg_entities_per_hour': zone['avg_entities_per_hour'],
            'bottleneck_score': zone['bottleneck_score'],
            'peak_hours': ', '.join(str(h) for h in zone.get('peak_hours', [])),
            'avg_transition_time': workflow_metrics.get('avg_transition_time', 0),
            'common_routes_count': len(workflow_metrics.get('common_routes', []))
        })
    
    return pd.DataFrame(data)

def _save_export_info(report_type: str, start_time: datetime, end_time: datetime, 
                     export_format: str, file_size: int) -> str:
    """Сохраняет информацию об экспорте в базу данных"""
    try:
        # Генерируем ID отчета на основе параметров
        report_id = f"{report_type}_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}"
        
        # Сохраняем экспорт
        export = store_export(
            report_id=report_id,
            export_format=export_format,
            file_path=f"/exports/{report_id}.{export_format}",
            file_size=file_size
        )
        
        return export['export_id']
    except Exception as e:
        logger.error(f"Error saving export info: {e}")
        return None